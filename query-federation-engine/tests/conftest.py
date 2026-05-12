"""Shared fixtures for the test suite."""

from __future__ import annotations

import pandas as pd
import pytest

from federation.catalog import ColumnDef, SchemaCatalog, SourceType, TableSchema
from federation.engine import FederationEngine


# ──────────────────────────────────────────────────────────────────────────────
# Sample DataFrames
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def orders_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id":         [1, 2, 3, 4, 5],
            "user_id":    [10, 20, 10, 30, 20],
            "total":      [99.9, 250.0, 30.0, 500.0, 75.5],
            "status":     ["shipped", "pending", "shipped", "cancelled", "shipped"],
            "created_at": pd.to_datetime(
                ["2024-01-10", "2024-02-01", "2024-03-15", "2024-04-01", "2024-04-20"]
            ),
        }
    )


@pytest.fixture()
def users_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id":      [10, 20, 30],
            "name":    ["Alice", "Bob", "Carol"],
            "country": ["US", "UK", "US"],
            "age":     [28, 35, 42],
        }
    )


@pytest.fixture()
def events_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id":   [1, 1, 2, 3, 5],
            "event_type": ["view", "purchase", "view", "purchase", "purchase"],
            "ts":         pd.to_datetime(
                ["2024-01-09", "2024-01-10", "2024-02-01", "2024-03-15", "2024-04-20"]
            ),
        }
    )


@pytest.fixture()
def products_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id":       [101, 102, 103],
            "name":     ["Widget", "Gadget", "Doohickey"],
            "category": ["electronics", "electronics", "hardware"],
            "price":    [19.99, 49.99, 9.99],
        }
    )


# ──────────────────────────────────────────────────────────────────────────────
# Catalog & engine fixture
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def catalog(orders_df, users_df, events_df, products_df) -> SchemaCatalog:
    cat = SchemaCatalog()

    cat.register_source("postgres",   SourceType.POSTGRES,   {})
    cat.register_source("mongodb",    SourceType.MONGODB,    {})
    cat.register_source("s3_parquet", SourceType.S3_PARQUET, {})
    cat.register_source("rest_api",   SourceType.REST_API,   {})

    def _schema(source, table, src_type, df, rows=None):
        cols = [ColumnDef(c, "string") for c in df.columns]
        return TableSchema(
            source=source, table=table, source_type=src_type,
            columns=cols, estimated_rows=rows or len(df),
        )

    cat.register_table(_schema("postgres",   "orders",   SourceType.POSTGRES,   orders_df))
    cat.register_table(_schema("mongodb",    "users",    SourceType.MONGODB,    users_df))
    cat.register_table(_schema("s3_parquet", "events",   SourceType.S3_PARQUET, events_df))
    cat.register_table(_schema("rest_api",   "products", SourceType.REST_API,   products_df))
    return cat


@pytest.fixture()
def engine(catalog, orders_df, users_df, events_df, products_df) -> FederationEngine:
    eng = FederationEngine(catalog)
    eng.register_mock_table("postgres",   "orders",   SourceType.POSTGRES,   orders_df)
    eng.register_mock_table("mongodb",    "users",    SourceType.MONGODB,    users_df)
    eng.register_mock_table("s3_parquet", "events",   SourceType.S3_PARQUET, events_df)
    eng.register_mock_table("rest_api",   "products", SourceType.REST_API,   products_df)
    return eng
