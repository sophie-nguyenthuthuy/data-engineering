"""End-to-end integration tests — full query federation across all four sources."""

import pandas as pd
import pytest

from federation.engine import FederationEngine


# ──────────────────────────────────────────────────────────────────────────────
# Single-source queries
# ──────────────────────────────────────────────────────────────────────────────

def test_single_source_select_all(engine, orders_df):
    df, stats = engine.query("SELECT * FROM postgres.orders")
    assert len(df) == len(orders_df)
    assert stats.rows_returned == len(orders_df)
    assert "postgres" in stats.sources_queried


def test_single_source_with_where(engine):
    df, _ = engine.query("SELECT id, status FROM postgres.orders WHERE status = 'shipped'")
    assert len(df) > 0
    # All returned rows must be shipped — check the column regardless of prefix
    status_col = [c for c in df.columns if "status" in c][0]
    assert all(df[status_col] == "shipped")


def test_single_source_limit(engine):
    df, _ = engine.query("SELECT id FROM postgres.orders LIMIT 2")
    assert len(df) == 2


def test_mongodb_equality_filter(engine):
    df, _ = engine.query("SELECT name, country FROM mongodb.users WHERE country = 'US'")
    assert len(df) == 2
    country_col = [c for c in df.columns if "country" in c][0]
    assert all(df[country_col] == "US")


def test_s3_parquet_filter(engine):
    df, _ = engine.query(
        "SELECT order_id, event_type FROM s3_parquet.events WHERE event_type = 'purchase'"
    )
    assert len(df) == 3
    et_col = [c for c in df.columns if "event_type" in c][0]
    assert all(df[et_col] == "purchase")


def test_rest_api_filter(engine):
    df, _ = engine.query(
        "SELECT name, category FROM rest_api.products WHERE category = 'electronics'"
    )
    assert len(df) == 2


# ──────────────────────────────────────────────────────────────────────────────
# Two-source joins
# ──────────────────────────────────────────────────────────────────────────────

def test_postgres_mongodb_join(engine):
    df, stats = engine.query(
        """
        SELECT o.id, u.name
        FROM postgres.orders o
        JOIN mongodb.users u ON o.user_id = u.id
        """
    )
    assert len(df) > 0
    assert "postgres" in stats.sources_queried
    assert "mongodb"  in stats.sources_queried
    # Should have both id and name columns
    col_names = " ".join(df.columns)
    assert "id"   in col_names
    assert "name" in col_names


def test_join_with_where_on_both_sides(engine):
    df, _ = engine.query(
        """
        SELECT o.id, o.total, u.name, u.country
        FROM postgres.orders o
        JOIN mongodb.users u ON o.user_id = u.id
        WHERE u.country = 'US'
        """
    )
    # Only US users: ids 10 (Alice) and 30 (Carol)
    assert len(df) > 0
    country_cols = [c for c in df.columns if "country" in c]
    if country_cols:
        assert all(df[country_cols[0]] == "US")


def test_postgres_s3_join(engine):
    df, stats = engine.query(
        """
        SELECT o.id, e.event_type
        FROM postgres.orders o
        JOIN s3_parquet.events e ON e.order_id = o.id
        """
    )
    assert len(df) > 0
    assert "s3_parquet" in stats.sources_queried


# ──────────────────────────────────────────────────────────────────────────────
# Three-source join
# ──────────────────────────────────────────────────────────────────────────────

def test_three_source_join(engine):
    df, stats = engine.query(
        """
        SELECT o.id, u.name, e.event_type
        FROM postgres.orders o
        JOIN mongodb.users u     ON o.user_id  = u.id
        JOIN s3_parquet.events e ON e.order_id = o.id
        """
    )
    assert len(df) > 0
    assert "postgres"   in stats.sources_queried
    assert "mongodb"    in stats.sources_queried
    assert "s3_parquet" in stats.sources_queried


# ──────────────────────────────────────────────────────────────────────────────
# All four sources
# ──────────────────────────────────────────────────────────────────────────────

def test_four_source_federation(engine):
    """Smoke test across all four data sources simultaneously."""
    df, stats = engine.query(
        """
        SELECT o.id, u.name, e.event_type, p.category
        FROM postgres.orders o
        JOIN mongodb.users u     ON o.user_id  = u.id
        JOIN s3_parquet.events e ON e.order_id = o.id
        JOIN rest_api.products p ON p.id       = o.id
        """
    )
    # Result may be empty (no matching IDs across all four), but must not error
    assert isinstance(df, pd.DataFrame)
    assert len(stats.sources_queried) == 4


# ──────────────────────────────────────────────────────────────────────────────
# Explain plan
# ──────────────────────────────────────────────────────────────────────────────

def test_explain_plan(engine):
    plan_text = engine.explain(
        "SELECT o.id, u.name FROM postgres.orders o JOIN mongodb.users u ON o.user_id = u.id"
    )
    assert "TableScan" in plan_text
    assert "postgres"  in plan_text
    assert "mongodb"   in plan_text


# ──────────────────────────────────────────────────────────────────────────────
# Stats
# ──────────────────────────────────────────────────────────────────────────────

def test_stats_populated(engine):
    _, stats = engine.query("SELECT id FROM postgres.orders")
    assert stats.total_time_ms > 0
    assert stats.rows_returned > 0
    assert "postgres.orders" in stats.rows_scanned


def test_stats_summary_string(engine):
    _, stats = engine.query("SELECT id FROM postgres.orders")
    summary = stats.summary()
    assert "Total time" in summary
    assert "Rows returned" in summary


# ──────────────────────────────────────────────────────────────────────────────
# register_mock_table helper
# ──────────────────────────────────────────────────────────────────────────────

def test_register_mock_table_dynamically():
    from federation.catalog import SourceType

    eng = FederationEngine.__new__(FederationEngine)
    from federation.catalog import SchemaCatalog
    from federation.planner import QueryPlanner, CostBasedOptimizer
    from federation.executor import Executor

    eng.catalog = SchemaCatalog()
    eng._planner = QueryPlanner(eng.catalog)
    eng._optimizer = CostBasedOptimizer()
    eng._executor = Executor(eng.catalog)

    df_mock = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
    eng.register_mock_table("postgres", "mytable", SourceType.POSTGRES, df_mock)

    result, _ = eng.query("SELECT id, val FROM postgres.mytable")
    assert len(result) == 2
