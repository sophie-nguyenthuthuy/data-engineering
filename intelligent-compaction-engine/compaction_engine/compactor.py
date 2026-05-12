"""
File compaction: merges small files into right-sized files.

For Delta Lake: wraps OPTIMIZE with optional partition filtering so only
affected partitions are rewritten, minimising I/O and lock duration.

For Iceberg: calls system.rewrite_data_files with bin-packing strategy.

The compactor never blocks reads — Delta OPTIMIZE and Iceberg rewrite are
copy-on-write operations that swap file pointers atomically.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from compaction_engine.analyzer import TableHealth

logger = logging.getLogger(__name__)


@dataclass
class CompactionResult:
    table_name: str
    table_format: str
    files_before: int
    files_after: int
    size_gb_before: float
    size_gb_after: float
    elapsed_seconds: float
    partitions_compacted: list[str] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def files_removed(self) -> int:
        return max(0, self.files_before - self.files_after)

    @property
    def reduction_pct(self) -> float:
        if self.files_before == 0:
            return 0.0
        return (self.files_removed / self.files_before) * 100

    def summary(self) -> str:
        if self.error:
            return f"FAILED [{self.table_name}]: {self.error}"
        return (
            f"[{self.table_name}] {self.files_before}→{self.files_after} files "
            f"({self.reduction_pct:.1f}% reduction) in {self.elapsed_seconds:.1f}s"
        )


class FileCompactor:
    """
    Compacts small files in Delta Lake and Iceberg tables.

    Strategy:
    - Delta: OPTIMIZE per partition batch (avoids full-table rewrites)
    - Iceberg: system.rewrite_data_files with bin-pack, targeting file size
    """

    def __init__(
        self,
        spark,
        target_file_size_mb: int = 128,
        small_file_size_mb: int = 32,
        max_partitions_per_run: int = 50,
    ):
        self.spark = spark
        self.target_file_size_mb = target_file_size_mb
        self.small_file_size_mb = small_file_size_mb
        self.max_partitions_per_run = max_partitions_per_run

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compact(
        self,
        health: TableHealth,
        partitions_filter: Optional[str] = None,
        dry_run: bool = False,
    ) -> CompactionResult:
        """Compact the table described by *health*, return stats."""
        if not health.needs_compaction:
            logger.info("Table %s does not need compaction", health.table_name)
            return self._noop_result(health)

        if health.table_format == "delta":
            return self._compact_delta(health, partitions_filter, dry_run)
        elif health.table_format == "iceberg":
            return self._compact_iceberg(health, dry_run)
        else:
            raise ValueError(f"Unsupported format: {health.table_format}")

    # ------------------------------------------------------------------
    # Delta compaction
    # ------------------------------------------------------------------

    def _compact_delta(
        self,
        health: TableHealth,
        partitions_filter: Optional[str],
        dry_run: bool,
    ) -> CompactionResult:
        self._set_delta_target_size()
        start = time.perf_counter()

        fragmented_partitions = self._find_fragmented_delta_partitions(health)
        batches = [
            fragmented_partitions[i : i + self.max_partitions_per_run]
            for i in range(0, len(fragmented_partitions), self.max_partitions_per_run)
        ]

        files_before = health.total_files
        compacted_partitions: list[str] = []

        if not batches:
            # No partitioned table or unpartitioned — optimize the whole table
            sql = self._delta_optimize_sql(health.table_name, None, partitions_filter)
            logger.info("Compaction SQL: %s", sql)
            if not dry_run:
                self.spark.sql(sql)
        else:
            for batch in batches:
                partition_predicate = self._partitions_to_predicate(batch)
                effective_filter = partitions_filter or partition_predicate
                sql = self._delta_optimize_sql(health.table_name, None, effective_filter)
                logger.info("Compaction SQL (batch): %s", sql)
                if not dry_run:
                    self.spark.sql(sql)
                compacted_partitions.extend(batch)

        elapsed = time.perf_counter() - start
        files_after = self._count_delta_files(health.table_name) if not dry_run else files_before

        return CompactionResult(
            table_name=health.table_name,
            table_format="delta",
            files_before=files_before,
            files_after=files_after,
            size_gb_before=health.total_size_gb,
            size_gb_after=health.total_size_gb,  # size stays same; layout improves
            elapsed_seconds=round(elapsed, 2),
            partitions_compacted=compacted_partitions,
        )

    def _set_delta_target_size(self) -> None:
        target_bytes = self.target_file_size_mb * 1024 * 1024
        self.spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(target_bytes))
        self.spark.conf.set(
            "spark.databricks.delta.optimize.minFileSize",
            str(self.small_file_size_mb * 1024 * 1024),
        )

    def _delta_optimize_sql(
        self,
        table_name: str,
        table_path: Optional[str],
        where_clause: Optional[str],
    ) -> str:
        target = f"delta.`{table_path}`" if table_path else table_name
        sql = f"OPTIMIZE {target}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        return sql

    def _find_fragmented_delta_partitions(self, health: TableHealth) -> list[str]:
        """Return partition specs that have disproportionately many small files."""
        if health.partition_count == 0:
            return []
        try:
            partitions_df = self.spark.sql(
                f"SHOW PARTITIONS {health.table_name}"
            )
            # Heuristic: compact the newest 20% of partitions first (most write activity)
            all_parts = [r[0] for r in partitions_df.collect()]
            cutoff = max(1, int(len(all_parts) * 0.2))
            return all_parts[-cutoff:]
        except Exception as e:
            logger.warning("Could not list partitions for %s: %s", health.table_name, e)
            return []

    def _partitions_to_predicate(self, partitions: list[str]) -> str:
        """Convert partition spec strings like 'dt=2024-01-01' to SQL predicates."""
        clauses = []
        for part in partitions:
            pairs = part.split("/")
            predicates = []
            for pair in pairs:
                if "=" in pair:
                    col, val = pair.split("=", 1)
                    predicates.append(f"{col} = '{val}'")
            if predicates:
                clauses.append("(" + " AND ".join(predicates) + ")")
        return " OR ".join(clauses) if clauses else ""

    def _count_delta_files(self, table_name: str) -> int:
        try:
            detail = self.spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
            return detail["numFiles"]
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Iceberg compaction
    # ------------------------------------------------------------------

    def _compact_iceberg(self, health: TableHealth, dry_run: bool) -> CompactionResult:
        target_bytes = self.target_file_size_mb * 1024 * 1024
        sql = (
            f"CALL spark_catalog.system.rewrite_data_files("
            f"  table => '{health.table_name}', "
            f"  strategy => 'binpack', "
            f"  options => map("
            f"    'target-file-size-bytes', '{target_bytes}', "
            f"    'min-file-size-bytes', '{self.small_file_size_mb * 1024 * 1024}'"
            f"  )"
            f")"
        )
        logger.info("Iceberg compaction SQL: %s", sql)

        start = time.perf_counter()
        if not dry_run:
            result = self.spark.sql(sql).collect()
            files_after = health.total_files
            if result:
                row = result[0].asDict()
                files_after = row.get("rewritten_data_files_count", health.total_files)
        else:
            files_after = health.total_files

        elapsed = time.perf_counter() - start
        return CompactionResult(
            table_name=health.table_name,
            table_format="iceberg",
            files_before=health.total_files,
            files_after=files_after,
            size_gb_before=health.total_size_gb,
            size_gb_after=health.total_size_gb,
            elapsed_seconds=round(elapsed, 2),
        )

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _noop_result(self, health: TableHealth) -> CompactionResult:
        return CompactionResult(
            table_name=health.table_name,
            table_format=health.table_format,
            files_before=health.total_files,
            files_after=health.total_files,
            size_gb_before=health.total_size_gb,
            size_gb_after=health.total_size_gb,
            elapsed_seconds=0.0,
        )
