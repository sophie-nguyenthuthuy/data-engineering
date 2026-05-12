"""Recommend partitioning strategies for BigQuery and Snowflake tables."""

from __future__ import annotations

import logging
import re
from collections import Counter

from ..models import Platform, Recommendation, RecommendationType, Severity, TableStats

logger = logging.getLogger(__name__)

# Partitioning on a date column typically saves 30-60 % of scan bytes.
# We use a conservative 30 % estimate.
_BQ_PARTITION_SAVINGS_PCT = 0.30
_SF_PARTITION_SAVINGS_PCT = 0.25  # Snowflake range-partitioning benefit

_MIN_SIZE_BYTES = 512 * 1024 ** 2  # 512 MiB minimum before recommending
_MIN_QUERY_COUNT = 5

# Common timestamp / date column name patterns
_DATE_PATTERNS = re.compile(
    r"(^|_)(date|dt|day|created|updated|timestamp|ts|event_date|report_date|"
    r"partition_date|load_date|ingest_date|event_time|created_at|updated_at)($|_)",
    re.IGNORECASE,
)

# Common high-cardinality ID columns not useful for partitioning
_BAD_PARTITION_COLS = {"id", "uuid", "guid", "pk", "row_id", "record_id"}


class PartitioningRecommender:
    """Analyse TableStats to surface partitioning recommendations."""

    def __init__(
        self,
        min_query_count: int = _MIN_QUERY_COUNT,
        min_size_bytes: int = _MIN_SIZE_BYTES,
    ) -> None:
        self.min_query_count = min_query_count
        self.min_size_bytes = min_size_bytes

    def recommend(self, table_stats: list[TableStats]) -> list[Recommendation]:
        recs: list[Recommendation] = []
        for tbl in table_stats:
            rec = self._evaluate(tbl)
            if rec:
                recs.append(rec)
        return recs

    # ------------------------------------------------------------------

    def _evaluate(self, tbl: TableStats) -> Recommendation | None:
        if tbl.query_count < self.min_query_count:
            return None
        if tbl.size_bytes > 0 and tbl.size_bytes < self.min_size_bytes:
            return None

        # Find the best partition column candidate from filter columns
        candidate = self._best_partition_column(tbl.filter_columns + tbl.group_by_columns)
        if not candidate:
            return None

        savings_pct = (
            _BQ_PARTITION_SAVINGS_PCT
            if tbl.platform == Platform.BIGQUERY
            else _SF_PARTITION_SAVINGS_PCT
        )
        monthly_savings = tbl.total_cost_usd * savings_pct
        severity = (
            Severity.HIGH
            if monthly_savings >= 50
            else (Severity.MEDIUM if monthly_savings >= 10 else Severity.LOW)
        )

        if tbl.platform == Platform.BIGQUERY:
            action = (
                f"-- Re-create with partitioning (BigQuery does not support ALTER TABLE ADD PARTITION):\n"
                f"CREATE OR REPLACE TABLE `{tbl.table_id}`\n"
                f"PARTITION BY DATE({candidate})\n"
                f"AS SELECT * FROM `{tbl.table_id}`;"
            )
            description = (
                f"`{tbl.table_id}` has no partition filter on `{candidate}`, "
                f"causing full-table scans across {tbl.query_count} queries "
                f"(${tbl.total_cost_usd:.2f} total). "
                f"Partitioning by `{candidate}` can cut scan cost by ~{savings_pct*100:.0f}%."
            )
        else:
            action = (
                f"-- Recreate with explicit clustering / partitioning:\n"
                f"CREATE OR REPLACE TABLE {tbl.table_id}\n"
                f"PARTITION BY ({candidate})\n"
                f"AS SELECT * FROM {tbl.table_id};"
            )
            description = (
                f"Table {tbl.table_id} is queried {tbl.query_count} times without "
                f"partition pruning on `{candidate}`. Partitioning reduces data scanned "
                f"by ~{savings_pct*100:.0f}%."
            )

        return Recommendation(
            rec_type=RecommendationType.PARTITIONING,
            platform=tbl.platform,
            severity=severity,
            table_id=tbl.table_id,
            title=f"Partition {tbl.table_id.split('.')[-1]} by {candidate}",
            description=description,
            action=action,
            estimated_savings_usd_monthly=monthly_savings,
            affected_query_count=tbl.query_count,
            evidence={
                "suggested_partition_column": candidate,
                "filter_columns_observed": tbl.filter_columns[:8],
                "table_size_bytes": tbl.size_bytes,
            },
        )

    # ------------------------------------------------------------------

    def _best_partition_column(self, columns: list[str]) -> str | None:
        """Return the most-frequent date-like column name, or None."""
        freq = Counter(columns)
        for col, _ in freq.most_common():
            if col.lower() in _BAD_PARTITION_COLS:
                continue
            if _DATE_PATTERNS.search(col):
                return col
        return None
