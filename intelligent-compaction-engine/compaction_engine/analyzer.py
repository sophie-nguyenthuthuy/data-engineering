"""
Query pattern analysis and table health inspection.

QueryPatternAnalyzer parses SQL query logs to score columns by access frequency,
which drives Z-order and clustering recommendations.

TableAnalyzer inspects Delta/Iceberg table metadata to surface file size
distributions, partition stats, and last-access times.
"""

from __future__ import annotations

import re
import sqlite3
import logging
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import sqlglot
import sqlglot.expressions as exp

logger = logging.getLogger(__name__)


@dataclass
class ColumnScore:
    column: str
    filter_count: int = 0
    join_count: int = 0
    group_count: int = 0
    order_count: int = 0

    @property
    def total_score(self) -> float:
        # Filters matter most for data skipping; joins second
        return (
            self.filter_count * 3.0
            + self.join_count * 2.0
            + self.group_count * 1.5
            + self.order_count * 1.0
        )


@dataclass
class TableHealth:
    table_name: str
    table_format: str  # "delta" or "iceberg"
    total_files: int
    small_files: int
    total_size_gb: float
    avg_file_size_mb: float
    min_file_size_mb: float
    max_file_size_mb: float
    partition_count: int
    stale_partition_count: int
    last_optimized: Optional[datetime]
    version: Optional[int] = None
    column_scores: dict[str, ColumnScore] = field(default_factory=dict)

    @property
    def fragmentation_ratio(self) -> float:
        if self.total_files == 0:
            return 0.0
        return self.small_files / self.total_files

    @property
    def needs_compaction(self) -> bool:
        return self.fragmentation_ratio > 0.3 or self.small_files > 10

    @property
    def needs_pruning(self) -> bool:
        return self.stale_partition_count > 0


class QueryPatternAnalyzer:
    """
    Parses SQL query logs and tracks per-column access patterns.

    Query logs can be fed as raw SQL strings, log files, or pulled from
    a Spark history server / audit log table.
    """

    def __init__(self, db_path: str = "compaction_metrics.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS query_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    query_text TEXT NOT NULL,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS column_access (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    access_type TEXT NOT NULL,  -- filter | join | group | order
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_col_table ON column_access(table_name)")
            conn.commit()

    def ingest_query(self, sql: str, table_name: Optional[str] = None) -> None:
        """Parse a SQL string and record column access patterns."""
        try:
            parsed = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.WARN)
        except Exception as e:
            logger.warning("Failed to parse SQL: %s — %s", sql[:120], e)
            return

        table_cols = self._extract_column_accesses(parsed)

        with sqlite3.connect(self.db_path) as conn:
            for tbl, col, access_type in table_cols:
                effective_table = table_name or tbl
                if effective_table:
                    conn.execute(
                        "INSERT INTO column_access(table_name, column_name, access_type) VALUES (?,?,?)",
                        (effective_table.lower(), col.lower(), access_type),
                    )
            conn.commit()

    def ingest_query_log_file(self, log_path: str, table_name: Optional[str] = None) -> int:
        """Read a file of SQL statements (one per line or semicolon-separated)."""
        path = Path(log_path)
        count = 0
        with path.open() as f:
            content = f.read()
        statements = [s.strip() for s in content.split(";") if s.strip()]
        for stmt in statements:
            self.ingest_query(stmt, table_name)
            count += 1
        logger.info("Ingested %d queries from %s", count, log_path)
        return count

    def _extract_column_accesses(
        self, tree: exp.Expression
    ) -> list[tuple[str, str, str]]:
        """Walk AST and tag columns by their role."""
        results: list[tuple[str, str, str]] = []

        table_aliases: dict[str, str] = {}
        for tbl in tree.find_all(exp.Table):
            alias = tbl.alias or tbl.name
            if alias:
                table_aliases[alias.lower()] = tbl.name.lower() if tbl.name else alias.lower()

        def resolve_table(col: exp.Column) -> str:
            tbl_ref = col.table
            if tbl_ref:
                return table_aliases.get(tbl_ref.lower(), tbl_ref.lower())
            # Return the first table found in the query as a fallback
            if table_aliases:
                return next(iter(table_aliases.values()))
            return ""

        # WHERE / ON filters
        for where in tree.find_all(exp.Where):
            for col in where.find_all(exp.Column):
                results.append((resolve_table(col), col.name, "filter"))

        # JOIN conditions
        for join in tree.find_all(exp.Join):
            for col in join.find_all(exp.Column):
                results.append((resolve_table(col), col.name, "join"))

        # GROUP BY
        for group in tree.find_all(exp.Group):
            for col in group.find_all(exp.Column):
                results.append((resolve_table(col), col.name, "group"))

        # ORDER BY
        for order in tree.find_all(exp.Order):
            for col in order.find_all(exp.Column):
                results.append((resolve_table(col), col.name, "order"))

        return results

    def get_column_scores(
        self, table_name: str, lookback_days: int = 30
    ) -> dict[str, ColumnScore]:
        """Return scored columns for a table based on recent query patterns."""
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=lookback_days)
        scores: dict[str, ColumnScore] = defaultdict(lambda: ColumnScore(column=""))

        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT column_name, access_type, COUNT(*) as cnt
                FROM column_access
                WHERE table_name = ? AND recorded_at >= ?
                GROUP BY column_name, access_type
                """,
                (table_name.lower(), cutoff.isoformat()),
            ).fetchall()

        for col_name, access_type, cnt in rows:
            score = scores[col_name]
            score.column = col_name
            if access_type == "filter":
                score.filter_count += cnt
            elif access_type == "join":
                score.join_count += cnt
            elif access_type == "group":
                score.group_count += cnt
            elif access_type == "order":
                score.order_count += cnt

        return dict(scores)

    def top_zorder_columns(
        self, table_name: str, max_cols: int = 4, min_frequency: int = 3, lookback_days: int = 30
    ) -> list[str]:
        """Return the highest-scoring columns for Z-ordering."""
        scores = self.get_column_scores(table_name, lookback_days)
        eligible = [s for s in scores.values() if s.total_score >= min_frequency]
        ranked = sorted(eligible, key=lambda s: s.total_score, reverse=True)
        return [s.column for s in ranked[:max_cols]]


class TableAnalyzer:
    """
    Inspects Delta Lake or Iceberg table metadata without touching data files.
    Works by querying Spark's metadata APIs or reading Delta log JSON files.
    """

    def __init__(self, spark, config: dict | None = None):
        self.spark = spark
        self.config = config or {}
        self.small_file_size_mb = self.config.get("small_file_size_mb", 32)
        self.stale_partition_days = self.config.get("stale_partition_days", 365)

    def analyze_delta_table(
        self, table_path: str, query_analyzer: Optional[QueryPatternAnalyzer] = None
    ) -> TableHealth:
        """Collect file and partition stats for a Delta table."""
        from delta.tables import DeltaTable

        dt = DeltaTable.forPath(self.spark, table_path)
        detail = dt.detail().collect()[0]
        history = dt.history(1).collect()[0]

        table_name = detail["name"] or Path(table_path).name

        # File stats via Delta's file manifest
        files_df = self.spark.sql(f"DESCRIBE DETAIL '{table_path}'")
        num_files = detail["numFiles"]
        size_bytes = detail["sizeInBytes"]
        total_size_gb = size_bytes / (1024 ** 3)
        avg_file_size_mb = (size_bytes / num_files / (1024 ** 2)) if num_files else 0

        # Small file count via the Delta log
        file_details = self.spark.read.format("delta").load(table_path).inputFiles()
        small_file_threshold_bytes = self.small_file_size_mb * 1024 * 1024

        # Approximate small file count using avg heuristic when full scan is expensive
        # A proper implementation reads _delta_log/*.json checkpoint files
        estimated_small = self._estimate_small_files(
            num_files, avg_file_size_mb, self.small_file_size_mb
        )

        # Partition stats
        partition_cols = detail["partitionColumns"]
        partition_count, stale_count = self._partition_stats_delta(
            table_path, partition_cols
        )

        last_optimized = self._last_optimize_time_delta(dt)

        health = TableHealth(
            table_name=table_name,
            table_format="delta",
            total_files=num_files,
            small_files=estimated_small,
            total_size_gb=total_size_gb,
            avg_file_size_mb=avg_file_size_mb,
            min_file_size_mb=0,
            max_file_size_mb=0,
            partition_count=partition_count,
            stale_partition_count=stale_count,
            last_optimized=last_optimized,
            version=history["version"],
        )

        if query_analyzer:
            health.column_scores = query_analyzer.get_column_scores(table_name)

        return health

    def analyze_iceberg_table(
        self, table_name: str, query_analyzer: Optional[QueryPatternAnalyzer] = None
    ) -> TableHealth:
        """Collect file and partition stats for an Iceberg table."""
        files_df = self.spark.sql(f"SELECT * FROM {table_name}.files")
        files_pd = files_df.select("file_size_in_bytes").toPandas()

        total_files = len(files_pd)
        total_bytes = files_pd["file_size_in_bytes"].sum()
        avg_mb = (total_bytes / total_files / (1024 ** 2)) if total_files else 0
        small_threshold = self.small_file_size_mb * 1024 * 1024
        small_files = int((files_pd["file_size_in_bytes"] < small_threshold).sum())
        total_size_gb = total_bytes / (1024 ** 3)

        partitions_df = self.spark.sql(f"SELECT * FROM {table_name}.partitions")
        partition_count = partitions_df.count()
        stale_count = self._stale_iceberg_partitions(table_name)

        health = TableHealth(
            table_name=table_name,
            table_format="iceberg",
            total_files=total_files,
            small_files=small_files,
            total_size_gb=total_size_gb,
            avg_file_size_mb=avg_mb,
            min_file_size_mb=float(files_pd["file_size_in_bytes"].min() / (1024 ** 2)) if total_files else 0,
            max_file_size_mb=float(files_pd["file_size_in_bytes"].max() / (1024 ** 2)) if total_files else 0,
            partition_count=partition_count,
            stale_partition_count=stale_count,
            last_optimized=None,
        )

        if query_analyzer:
            health.column_scores = query_analyzer.get_column_scores(table_name)

        return health

    def _estimate_small_files(
        self, total_files: int, avg_size_mb: float, threshold_mb: float
    ) -> int:
        """Heuristic small-file count (assumes log-normal distribution)."""
        if avg_size_mb == 0:
            return 0
        ratio = min(1.0, threshold_mb / avg_size_mb) if avg_size_mb > threshold_mb else 1.0
        if avg_size_mb >= threshold_mb * 2:
            return max(0, int(total_files * 0.05))
        elif avg_size_mb >= threshold_mb:
            return max(0, int(total_files * 0.2))
        else:
            return max(0, int(total_files * 0.75))

    def _partition_stats_delta(
        self, table_path: str, partition_cols: list
    ) -> tuple[int, int]:
        if not partition_cols:
            return 0, 0
        try:
            partitions = self.spark.sql(
                f"SHOW PARTITIONS delta.`{table_path}`"
            )
            count = partitions.count()
            stale_cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=self.stale_partition_days)
            # Heuristic: flag partitions whose date-encoded value predates cutoff
            stale = self._estimate_stale_partitions(partitions, stale_cutoff)
            return count, stale
        except Exception:
            return 0, 0

    def _estimate_stale_partitions(self, partitions_df, cutoff: datetime) -> int:
        """Count partitions whose name encodes a date older than cutoff."""
        date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2}|\d{8})")
        stale = 0
        for row in partitions_df.toLocalIterator():
            partition_str = str(row[0])
            match = date_pattern.search(partition_str)
            if match:
                raw = match.group(1).replace("-", "")
                try:
                    dt = datetime.strptime(raw, "%Y%m%d")
                    if dt < cutoff:
                        stale += 1
                except ValueError:
                    pass
        return stale

    def _last_optimize_time_delta(self, dt) -> Optional[datetime]:
        try:
            history = dt.history(50).filter("operation = 'OPTIMIZE'")
            if history.count() == 0:
                return None
            ts = history.select("timestamp").first()["timestamp"]
            return ts
        except Exception:
            return None

    def _stale_iceberg_partitions(self, table_name: str) -> int:
        try:
            partitions = self.spark.sql(
                f"SELECT spec_id, partition, record_count, last_updated_ms FROM {table_name}.partitions"
            )
            cutoff_ms = (
                datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=self.stale_partition_days)
            ).timestamp() * 1000
            stale = partitions.filter(f"last_updated_ms < {cutoff_ms}").count()
            return stale
        except Exception:
            return 0
