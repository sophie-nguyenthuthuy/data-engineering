"""Tests for PerformanceMetrics."""

import pytest
from unittest.mock import MagicMock
from compaction_engine.metrics import (
    PerformanceMetrics,
    BenchmarkQuery,
    BenchmarkRun,
    QueryTiming,
    CompactionImpact,
)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test_metrics.db")


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    # Simulate fast query returning 100 rows
    df_mock = MagicMock()
    df_mock.count.return_value = 100
    spark.sql.return_value = df_mock
    spark.catalog = MagicMock()
    return spark


@pytest.fixture
def metrics(mock_spark, tmp_db):
    return PerformanceMetrics(
        spark=mock_spark,
        db_path=tmp_db,
        benchmark_runs=2,
        prometheus_port=None,
    )


QUERIES = [
    BenchmarkQuery(
        name="filter_by_date",
        sql="SELECT COUNT(*) FROM events WHERE event_date = '2024-01-01'",
    ),
    BenchmarkQuery(
        name="filter_by_region",
        sql="SELECT COUNT(*) FROM events WHERE region = 'us-east'",
    ),
]


class TestBenchmarkRun:
    def test_run_benchmark_returns_run(self, metrics):
        run = metrics.run_benchmark(
            "events", QUERIES, phase="before",
            file_count=100, avg_file_size_mb=20.0, total_size_gb=2.0,
        )
        assert run.phase == "before"
        assert len(run.timings) == len(QUERIES)

    def test_timings_have_positive_elapsed(self, metrics):
        run = metrics.run_benchmark("events", QUERIES, phase="before")
        for t in run.timings:
            assert t.elapsed_seconds >= 0

    def test_avg_query_time(self, metrics):
        run = metrics.run_benchmark("events", QUERIES, phase="before")
        assert run.avg_query_time >= 0

    def test_run_is_persisted(self, metrics, tmp_db):
        import sqlite3
        run = metrics.run_benchmark("events", QUERIES, phase="before")
        with sqlite3.connect(tmp_db) as conn:
            rows = conn.execute(
                "SELECT run_id FROM benchmark_runs WHERE table_name='events'"
            ).fetchall()
        assert len(rows) >= 1


class TestCompactionImpact:
    def _make_run(self, phase, avg_time, file_count):
        timings = [
            QueryTiming("q1", elapsed_seconds=avg_time, rows_returned=100),
        ]
        return BenchmarkRun(
            table_name="events",
            phase=phase,
            timings=timings,
            file_count=file_count,
            avg_file_size_mb=20.0,
            total_size_gb=5.0,
        )

    def test_query_speedup_pct(self):
        before = self._make_run("before", avg_time=2.0, file_count=200)
        after = self._make_run("after", avg_time=0.5, file_count=50)
        impact = CompactionImpact("events", before, after)
        # (2.0 - 0.5) / 2.0 * 100 = 75%
        assert impact.query_speedup_pct == pytest.approx(75.0)

    def test_file_reduction_pct(self):
        before = self._make_run("before", avg_time=2.0, file_count=200)
        after = self._make_run("after", avg_time=0.5, file_count=50)
        impact = CompactionImpact("events", before, after)
        assert impact.file_reduction_pct == pytest.approx(75.0)

    def test_report_contains_key_metrics(self):
        before = self._make_run("before", avg_time=2.0, file_count=200)
        after = self._make_run("after", avg_time=0.5, file_count=50)
        impact = CompactionImpact("events", before, after)
        report = impact.report()
        assert "events" in report
        assert "75.0%" in report or "+75" in report

    def test_compare_persists_event(self, metrics, tmp_db):
        import sqlite3
        before = self._make_run("before", avg_time=1.5, file_count=150)
        after = self._make_run("after", avg_time=0.8, file_count=60)
        metrics.compare(before, after)
        with sqlite3.connect(tmp_db) as conn:
            rows = conn.execute(
                "SELECT query_speedup_pct FROM compaction_events WHERE table_name='events'"
            ).fetchall()
        assert len(rows) >= 1
        assert rows[0][0] > 0

    def test_history_returns_records(self, metrics, tmp_db):
        import sqlite3
        before = self._make_run("before", avg_time=1.5, file_count=150)
        after = self._make_run("after", avg_time=0.8, file_count=60)
        metrics.compare(before, after)
        history = metrics.history("events")
        assert len(history) >= 1
        assert "query_speedup_pct" in history[0]
