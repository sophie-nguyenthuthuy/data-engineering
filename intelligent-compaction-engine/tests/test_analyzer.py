"""Tests for QueryPatternAnalyzer."""

import os
import tempfile
import pytest
from compaction_engine.analyzer import QueryPatternAnalyzer, ColumnScore


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test_metrics.db")


@pytest.fixture
def analyzer(tmp_db):
    return QueryPatternAnalyzer(db_path=tmp_db)


class TestQueryIngestion:
    def test_ingest_simple_filter(self, analyzer):
        analyzer.ingest_query(
            "SELECT * FROM events WHERE event_date = '2024-01-01'",
            table_name="events",
        )
        scores = analyzer.get_column_scores("events")
        assert "event_date" in scores
        assert scores["event_date"].filter_count >= 1

    def test_ingest_join_columns(self, analyzer):
        analyzer.ingest_query(
            "SELECT a.id, b.name FROM orders a JOIN customers b ON a.customer_id = b.id",
        )
        scores = analyzer.get_column_scores("orders")
        # customer_id appears in ON clause which is treated as join
        assert any(s.join_count > 0 for s in scores.values())

    def test_ingest_group_by(self, analyzer):
        analyzer.ingest_query(
            "SELECT region, COUNT(*) FROM sales GROUP BY region",
            table_name="sales",
        )
        scores = analyzer.get_column_scores("sales")
        assert "region" in scores
        assert scores["region"].group_count >= 1

    def test_ingest_order_by(self, analyzer):
        analyzer.ingest_query(
            "SELECT * FROM logs ORDER BY created_at DESC",
            table_name="logs",
        )
        scores = analyzer.get_column_scores("logs")
        assert "created_at" in scores
        assert scores["created_at"].order_count >= 1

    def test_invalid_sql_does_not_raise(self, analyzer):
        # Should log a warning but not raise
        analyzer.ingest_query("THIS IS NOT SQL", table_name="t")

    def test_multiple_queries_accumulate(self, analyzer):
        for _ in range(5):
            analyzer.ingest_query(
                "SELECT * FROM orders WHERE status = 'open'",
                table_name="orders",
            )
        scores = analyzer.get_column_scores("orders")
        assert scores["status"].filter_count == 5


class TestColumnScoring:
    def test_total_score_weights(self):
        s = ColumnScore(column="c", filter_count=2, join_count=1, group_count=1, order_count=1)
        # filter*3 + join*2 + group*1.5 + order*1 = 6+2+1.5+1 = 10.5
        assert s.total_score == pytest.approx(10.5)

    def test_top_zorder_columns_respects_max(self, analyzer):
        for col in ["a", "b", "c", "d", "e"]:
            for _ in range(5):
                analyzer.ingest_query(
                    f"SELECT * FROM t WHERE {col} = 1", table_name="t"
                )
        top = analyzer.top_zorder_columns("t", max_cols=3, min_frequency=1)
        assert len(top) <= 3

    def test_top_zorder_columns_min_frequency_filters(self, analyzer):
        analyzer.ingest_query(
            "SELECT * FROM t WHERE rare_col = 1", table_name="t"
        )
        top = analyzer.top_zorder_columns("t", max_cols=4, min_frequency=5)
        # rare_col only appeared once, score=3; below threshold of 5
        assert "rare_col" not in top

    def test_columns_ranked_by_score(self, analyzer):
        # high_col filtered 10x, low_col filtered 2x
        for _ in range(10):
            analyzer.ingest_query(
                "SELECT * FROM t WHERE high_col = 1", table_name="t"
            )
        for _ in range(2):
            analyzer.ingest_query(
                "SELECT * FROM t WHERE low_col = 1", table_name="t"
            )
        top = analyzer.top_zorder_columns("t", max_cols=4, min_frequency=1)
        assert top[0] == "high_col"


class TestLogFileIngestion:
    def test_ingest_log_file(self, analyzer, tmp_path):
        log_file = tmp_path / "queries.sql"
        log_file.write_text(
            "SELECT * FROM events WHERE dt = '2024-01-01';\n"
            "SELECT * FROM events WHERE dt = '2024-01-02' AND region = 'us-east';\n"
        )
        count = analyzer.ingest_query_log_file(str(log_file), table_name="events")
        assert count == 2
        scores = analyzer.get_column_scores("events")
        assert "dt" in scores
        assert scores["dt"].filter_count == 2
