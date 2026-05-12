"""Recommend clustering keys for BigQuery / Snowflake tables."""

from __future__ import annotations

import logging
from collections import Counter

from ..models import Platform, QueryRecord, Recommendation, RecommendationType, Severity, TableStats

logger = logging.getLogger(__name__)

# BigQuery: clustering saves ~20-40% on tables where filters align with cluster key.
# We use a conservative 20% estimate.
_BQ_CLUSTER_SAVINGS_PCT = 0.20
# Snowflake: micro-partition pruning via clustering keys saves ~15-30%.
_SF_CLUSTER_SAVINGS_PCT = 0.18

# Minimum table size to bother recommending clustering (1 GiB)
_MIN_SIZE_BYTES = 1024 ** 3
# Minimum queries referencing the table
_MIN_QUERY_COUNT = 5


class ClusteringRecommender:
    """Analyse TableStats to surface clustering-key recommendations."""

    def __init__(self, min_query_count: int = _MIN_QUERY_COUNT, min_size_bytes: int = _MIN_SIZE_BYTES) -> None:
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

        # Score candidate clustering columns by frequency across filter + join columns
        candidates = tbl.filter_columns + tbl.join_columns
        if not candidates:
            return None

        freq = Counter(candidates)
        # Pick top-4 columns (BQ supports up to 4 clustering columns)
        top_cols = [col for col, _ in freq.most_common(4)]
        if not top_cols:
            return None

        savings_pct = (
            _BQ_CLUSTER_SAVINGS_PCT if tbl.platform == Platform.BIGQUERY else _SF_CLUSTER_SAVINGS_PCT
        )
        monthly_savings = tbl.total_cost_usd * savings_pct

        severity = Severity.HIGH if monthly_savings >= 50 else (Severity.MEDIUM if monthly_savings >= 10 else Severity.LOW)

        if tbl.platform == Platform.BIGQUERY:
            action = (
                f"ALTER TABLE `{tbl.table_id}` "
                f"CLUSTER BY {', '.join(top_cols[:4])};"
            )
            description = (
                f"Table `{tbl.table_id}` is scanned {tbl.query_count} times and costs "
                f"${tbl.total_cost_usd:.2f} total. Adding clustering on "
                f"({', '.join(top_cols)}) can reduce scan costs by ~{savings_pct*100:.0f}%."
            )
        else:
            action = (
                f"ALTER TABLE {tbl.table_id} "
                f"CLUSTER BY ({', '.join(top_cols[:4])});"
            )
            description = (
                f"Table {tbl.table_id} is queried {tbl.query_count} times. "
                f"Clustering on ({', '.join(top_cols)}) improves micro-partition pruning, "
                f"saving ~{savings_pct*100:.0f}% of scan costs."
            )

        return Recommendation(
            rec_type=RecommendationType.CLUSTERING,
            platform=tbl.platform,
            severity=severity,
            table_id=tbl.table_id,
            title=f"Add clustering key to {tbl.table_id.split('.')[-1]}",
            description=description,
            action=action,
            estimated_savings_usd_monthly=monthly_savings,
            affected_query_count=tbl.query_count,
            evidence={
                "top_filter_columns": tbl.filter_columns[:6],
                "top_join_columns": tbl.join_columns[:6],
                "suggested_cluster_keys": top_cols,
                "table_size_bytes": tbl.size_bytes,
            },
        )
