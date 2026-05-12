"""Tests for FileCompactor."""

import pytest
from unittest.mock import MagicMock, patch
from compaction_engine.analyzer import TableHealth
from compaction_engine.compactor import FileCompactor, CompactionResult


def make_health(
    table_format="delta",
    total_files=200,
    small_files=80,
    avg_file_size_mb=20.0,
):
    return TableHealth(
        table_name="db.events",
        table_format=table_format,
        total_files=total_files,
        small_files=small_files,
        total_size_gb=5.0,
        avg_file_size_mb=avg_file_size_mb,
        min_file_size_mb=1.0,
        max_file_size_mb=256.0,
        partition_count=30,
        stale_partition_count=5,
        last_optimized=None,
    )


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.conf = MagicMock()
    result_mock = MagicMock()
    result_mock.collect.return_value = [MagicMock(numFiles=50)]
    spark.sql.return_value = result_mock
    return spark


@pytest.fixture
def compactor(mock_spark):
    return FileCompactor(
        spark=mock_spark,
        target_file_size_mb=128,
        small_file_size_mb=32,
    )


class TestCompactionDecisions:
    def test_skips_healthy_table(self, compactor, mock_spark):
        # 5% fragmentation — does not need compaction
        health = make_health(total_files=100, small_files=5, avg_file_size_mb=100.0)
        result = compactor.compact(health)
        # OPTIMIZE should not have been called
        mock_spark.sql.assert_not_called()
        assert result.files_before == result.files_after

    def test_compacts_fragmented_table(self, compactor, mock_spark):
        health = make_health(total_files=200, small_files=80)
        result = compactor.compact(health, dry_run=True)
        # Dry run returns before == after but should produce a result object
        assert isinstance(result, CompactionResult)

    def test_unsupported_format_raises(self, compactor):
        health = make_health()
        health.table_format = "parquet"
        with pytest.raises(ValueError, match="Unsupported format"):
            compactor.compact(health)


class TestDeltaCompaction:
    def test_dry_run_no_spark_sql(self, compactor, mock_spark):
        health = make_health(table_format="delta", total_files=150, small_files=60)
        compactor.compact(health, dry_run=True)
        # In dry_run mode, spark.sql should not be called for actual OPTIMIZE
        # (it may be called for partition listing)
        optimize_calls = [
            call for call in mock_spark.sql.call_args_list
            if "OPTIMIZE" in str(call)
        ]
        assert len(optimize_calls) == 0

    def test_sets_target_file_size(self, compactor, mock_spark):
        health = make_health(table_format="delta")
        compactor.compact(health, dry_run=False)
        set_calls = str(mock_spark.conf.set.call_args_list)
        assert "optimize.maxFileSize" in set_calls


class TestIcebergCompaction:
    def test_generates_correct_sql(self, compactor, mock_spark):
        health = make_health(table_format="iceberg", total_files=100, small_files=50)
        mock_spark.sql.return_value.collect.return_value = [
            MagicMock(**{"asDict.return_value": {"rewritten_data_files_count": 30}})
        ]
        result = compactor.compact(health, dry_run=False)
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any("rewrite_data_files" in c for c in sql_calls)

    def test_iceberg_dry_run_skips_sql(self, compactor, mock_spark):
        health = make_health(table_format="iceberg")
        compactor.compact(health, dry_run=True)
        optimize_calls = [c for c in mock_spark.sql.call_args_list if "rewrite" in str(c)]
        assert len(optimize_calls) == 0


class TestCompactionResult:
    def test_files_removed_calculation(self):
        r = CompactionResult(
            table_name="t",
            table_format="delta",
            files_before=100,
            files_after=30,
            size_gb_before=5.0,
            size_gb_after=5.0,
            elapsed_seconds=12.3,
        )
        assert r.files_removed == 70
        assert r.reduction_pct == pytest.approx(70.0)

    def test_summary_format(self):
        r = CompactionResult(
            table_name="db.events",
            table_format="delta",
            files_before=100,
            files_after=40,
            size_gb_before=5.0,
            size_gb_after=5.0,
            elapsed_seconds=30.0,
        )
        summary = r.summary()
        assert "db.events" in summary
        assert "60.0%" in summary
