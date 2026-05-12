"""Tests for ZOrderOptimizer."""

import pytest
from unittest.mock import MagicMock, patch
from compaction_engine.analyzer import TableHealth, ColumnScore
from compaction_engine.optimizer import ZOrderOptimizer, ZOrderPlan


def make_health(
    table_name="test_table",
    table_format="delta",
    col_scores: dict | None = None,
    total_files=100,
    small_files=40,
):
    scores = col_scores or {
        "event_date": ColumnScore("event_date", filter_count=10, join_count=0),
        "region": ColumnScore("region", filter_count=7, join_count=2),
        "user_id": ColumnScore("user_id", filter_count=2, join_count=8),
        "status": ColumnScore("status", filter_count=1),
    }
    return TableHealth(
        table_name=table_name,
        table_format=table_format,
        total_files=total_files,
        small_files=small_files,
        total_size_gb=10.0,
        avg_file_size_mb=20.0,
        min_file_size_mb=1.0,
        max_file_size_mb=128.0,
        partition_count=12,
        stale_partition_count=2,
        last_optimized=None,
        column_scores=scores,
    )


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    spark.sql.return_value.collect.return_value = []
    return spark


@pytest.fixture
def mock_analyzer():
    analyzer = MagicMock()
    analyzer.top_zorder_columns.return_value = ["event_date", "region", "user_id"]
    return analyzer


@pytest.fixture
def optimizer(mock_spark, mock_analyzer):
    return ZOrderOptimizer(
        spark=mock_spark,
        query_analyzer=mock_analyzer,
        max_zorder_columns=4,
        min_column_frequency=3,
    )


class TestZOrderPlanGeneration:
    def test_recommend_delta_sql(self, optimizer, mock_analyzer):
        health = make_health()
        plan = optimizer.recommend(health)
        assert "OPTIMIZE" in plan.sql_command
        assert "ZORDER BY" in plan.sql_command

    def test_recommend_iceberg_sql(self, optimizer, mock_analyzer):
        health = make_health(table_format="iceberg")
        plan = optimizer.recommend(health)
        assert "rewrite_data_files" in plan.sql_command
        assert "sort" in plan.sql_command

    def test_should_execute_when_columns_exist(self, optimizer):
        health = make_health()
        plan = optimizer.recommend(health)
        assert plan.should_execute is True

    def test_should_not_execute_when_no_columns(self, optimizer, mock_analyzer):
        mock_analyzer.top_zorder_columns.return_value = []
        health = make_health(col_scores={})
        plan = optimizer.recommend(health)
        assert plan.should_execute is False

    def test_speedup_estimate_high(self, optimizer):
        high_score_cols = {
            "event_date": ColumnScore("event_date", filter_count=30),
        }
        health = make_health(col_scores=high_score_cols)
        plan = optimizer.recommend(health)
        assert plan.estimated_speedup == "high"

    def test_speedup_estimate_low(self, optimizer, mock_analyzer):
        mock_analyzer.top_zorder_columns.return_value = ["rare_col"]
        low_score_cols = {
            "rare_col": ColumnScore("rare_col", filter_count=1),
        }
        health = make_health(col_scores=low_score_cols)
        plan = optimizer.recommend(health)
        assert plan.estimated_speedup == "low"


class TestZOrderExecution:
    def test_dry_run_returns_sql(self, optimizer):
        plan = ZOrderPlan(
            table_name="t",
            table_format="delta",
            recommended_columns=["col1"],
            current_columns=[],
            score_delta=10.0,
            estimated_speedup="high",
            sql_command="OPTIMIZE t ZORDER BY (col1)",
        )
        result = optimizer.execute(plan, dry_run=True)
        assert result["dry_run"] is True
        assert "sql" in result

    def test_skip_when_no_benefit(self, optimizer):
        plan = ZOrderPlan(
            table_name="t",
            table_format="delta",
            recommended_columns=[],
            current_columns=[],
            score_delta=0.0,
            estimated_speedup="none",
            sql_command="",
        )
        result = optimizer.execute(plan, dry_run=False)
        assert result["skipped"] is True

    def test_execute_calls_spark_sql(self, optimizer, mock_spark):
        plan = ZOrderPlan(
            table_name="t",
            table_format="delta",
            recommended_columns=["col1"],
            current_columns=[],
            score_delta=5.0,
            estimated_speedup="moderate",
            sql_command="OPTIMIZE t ZORDER BY (col1)",
        )
        optimizer.execute(plan, dry_run=False)
        mock_spark.sql.assert_called()
