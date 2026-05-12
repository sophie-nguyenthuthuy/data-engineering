"""Unit tests for recommenders."""

import pytest
from query_cost_optimizer.models import Platform, TableStats, QueryRecord
from query_cost_optimizer.recommenders.clustering import ClusteringRecommender
from query_cost_optimizer.recommenders.partitioning import PartitioningRecommender
from query_cost_optimizer.recommenders.patterns import PatternDetector
from datetime import datetime, timezone


def _make_table(
    table_id="proj.ds.events",
    platform=Platform.BIGQUERY,
    size_bytes=5 * 1024 ** 3,
    query_count=50,
    total_cost_usd=500.0,
    filter_cols=None,
    join_cols=None,
    group_by_cols=None,
) -> TableStats:
    return TableStats(
        table_id=table_id,
        platform=platform,
        row_count=100_000_000,
        size_bytes=size_bytes,
        query_count=query_count,
        total_bytes_scanned=size_bytes * query_count,
        total_cost_usd=total_cost_usd,
        filter_columns=["event_date", "user_id"] if filter_cols is None else filter_cols,
        join_columns=["user_id"] if join_cols is None else join_cols,
        group_by_columns=["event_date"] if group_by_cols is None else group_by_cols,
    )


def _make_record(sql: str, cost: float = 1.0, platform=Platform.BIGQUERY) -> QueryRecord:
    now = datetime.now(timezone.utc)
    return QueryRecord(
        query_id="q1",
        query_text=sql,
        user="user@example.com",
        start_time=now,
        end_time=now,
        bytes_processed=1024 ** 3,
        bytes_billed=1024 ** 3,
        elapsed_ms=5000,
        tables_referenced=["proj.ds.events"],
        platform=platform,
        cost_usd=cost,
    )


# ── Clustering ────────────────────────────────────────────────────────────────

class TestClusteringRecommender:
    def test_recommends_on_large_table(self):
        tbl = _make_table()
        recs = ClusteringRecommender(min_query_count=5).recommend([tbl])
        assert len(recs) == 1
        assert "event_date" in recs[0].action or "user_id" in recs[0].action

    def test_skips_small_table(self):
        tbl = _make_table(size_bytes=10 * 1024 ** 2)  # 10 MiB
        recs = ClusteringRecommender().recommend([tbl])
        assert recs == []

    def test_skips_low_query_count(self):
        tbl = _make_table(query_count=2)
        recs = ClusteringRecommender(min_query_count=5).recommend([tbl])
        assert recs == []

    def test_no_filter_columns_skipped(self):
        tbl = _make_table(filter_cols=[], join_cols=[])
        recs = ClusteringRecommender().recommend([tbl])
        assert recs == []

    def test_snowflake_action_format(self):
        tbl = _make_table(platform=Platform.SNOWFLAKE)
        recs = ClusteringRecommender(min_query_count=5).recommend([tbl])
        assert len(recs) == 1
        assert "CLUSTER BY" in recs[0].action


# ── Partitioning ──────────────────────────────────────────────────────────────

class TestPartitioningRecommender:
    def test_recommends_date_column(self):
        tbl = _make_table(filter_cols=["event_date", "user_id"])
        recs = PartitioningRecommender(min_query_count=5).recommend([tbl])
        assert len(recs) == 1
        assert "event_date" in recs[0].action

    def test_no_date_column_skipped(self):
        tbl = _make_table(filter_cols=["user_id", "product_id"], group_by_cols=["status"])
        recs = PartitioningRecommender().recommend([tbl])
        assert recs == []

    def test_skips_small_table(self):
        tbl = _make_table(size_bytes=100 * 1024 ** 2)  # 100 MiB
        recs = PartitioningRecommender().recommend([tbl])
        assert recs == []


# ── Pattern detector ──────────────────────────────────────────────────────────

class TestPatternDetector:
    def test_detects_select_star(self):
        records = [
            _make_record("SELECT * FROM events WHERE event_date = '2024-01-01'", cost=2.0)
            for _ in range(5)
        ]
        patterns = PatternDetector(min_query_count=3).detect(records)
        names = [p.pattern_name for p in patterns]
        assert any("SELECT *" in n for n in names)

    def test_below_min_count_not_flagged(self):
        records = [_make_record("SELECT * FROM events", cost=1.0) for _ in range(2)]
        patterns = PatternDetector(min_query_count=3).detect(records)
        assert patterns == []

    def test_clean_queries_no_patterns(self):
        records = [
            _make_record(
                "SELECT id, name FROM events WHERE event_date >= '2024-01-01' LIMIT 100",
                cost=0.5,
            )
            for _ in range(10)
        ]
        patterns = PatternDetector(min_query_count=3).detect(records)
        assert patterns == []
