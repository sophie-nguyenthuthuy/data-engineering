"""Tests for PartitionPruner."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call
from compaction_engine.analyzer import TableHealth
from compaction_engine.pruner import PartitionPruner, PruningResult


def make_health(table_format="delta", partition_count=100, stale_count=20):
    return TableHealth(
        table_name="db.sales",
        table_format=table_format,
        total_files=500,
        small_files=50,
        total_size_gb=20.0,
        avg_file_size_mb=40.0,
        min_file_size_mb=5.0,
        max_file_size_mb=256.0,
        partition_count=partition_count,
        stale_partition_count=stale_count,
        last_optimized=None,
    )


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.conf = MagicMock()
    spark.sql.return_value = MagicMock()
    spark.sql.return_value.collect.return_value = []
    return spark


@pytest.fixture
def pruner(mock_spark):
    return PartitionPruner(
        spark=mock_spark,
        stale_partition_days=365,
        vacuum_retain_hours=168,
        auto_archive_days=730,
        dry_run=False,
    )


@pytest.fixture
def dry_pruner(mock_spark):
    return PartitionPruner(
        spark=mock_spark,
        stale_partition_days=365,
        vacuum_retain_hours=168,
        auto_archive_days=730,
        dry_run=True,
    )


class TestPartitionAgeDetection:
    def test_identifies_old_date_partition(self, pruner):
        # dt=2021-01-01 is > 730 days old → should be in drop list
        old_date = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=800)).strftime("%Y-%m-%d")
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda s, i: f"dt={old_date}"
        pruner.spark.sql.return_value.collect.return_value = [mock_row]

        stale = pruner._identify_stale_delta_partitions(make_health())
        assert any(s["age_days"] >= 730 for s in stale)

    def test_ignores_recent_partitions(self, pruner):
        recent_date = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=10)).strftime("%Y-%m-%d")
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda s, i: f"dt={recent_date}"
        pruner.spark.sql.return_value.collect.return_value = [mock_row]

        stale = pruner._identify_stale_delta_partitions(make_health())
        assert len(stale) == 0


class TestDeltaPruning:
    def test_dry_run_does_not_drop(self, dry_pruner, mock_spark):
        old_date = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=800)).strftime("%Y-%m-%d")
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda s, i: f"dt={old_date}"
        mock_spark.sql.return_value.collect.return_value = [mock_row]

        health = make_health()
        result = dry_pruner.prune(health)
        # In dry_run, partitions_dropped should have [dry-run] prefix
        drop_calls = [c for c in str(mock_spark.sql.call_args_list) if "DROP" in c]
        assert len(drop_calls) == 0

    def test_vacuum_generates_correct_sql(self, pruner, mock_spark):
        pruner.vacuum("db.sales", "delta")
        calls = str(mock_spark.sql.call_args_list)
        assert "VACUUM" in calls
        assert "168" in calls

    def test_result_has_correct_format(self, pruner, mock_spark):
        mock_spark.sql.return_value.collect.return_value = []
        health = make_health(partition_count=0, stale_count=0)
        result = pruner.prune(health)
        assert isinstance(result, PruningResult)
        assert result.table_name == "db.sales"


class TestIcebergPruning:
    def test_calls_expire_snapshots(self, pruner, mock_spark):
        health = make_health(table_format="iceberg")
        pruner.prune(health)
        calls = str(mock_spark.sql.call_args_list)
        assert "expire_snapshots" in calls

    def test_calls_delete_orphan_files(self, pruner, mock_spark):
        health = make_health(table_format="iceberg")
        pruner.prune(health)
        calls = str(mock_spark.sql.call_args_list)
        assert "delete_orphan_files" in calls


class TestPruningResult:
    def test_summary_format(self):
        r = PruningResult(
            table_name="db.sales",
            table_format="delta",
            partitions_dropped=["dt=2021-01-01", "dt=2021-02-01"],
            bytes_reclaimed=1024 ** 3,
            elapsed_seconds=5.0,
        )
        summary = r.summary()
        assert "db.sales" in summary
        assert "dropped=2" in summary
        assert "1.00 GB" in summary

    def test_error_summary(self):
        r = PruningResult(table_name="t", table_format="delta", error="connection refused")
        assert "FAILED" in r.summary()
