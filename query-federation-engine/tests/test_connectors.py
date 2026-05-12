"""Tests for individual connectors in mock mode."""

import pandas as pd
import pytest
import sqlglot
import sqlglot.expressions as exp

from federation.connectors import (
    MongoDBConnector, PostgresConnector,
    RestApiConnector, S3ParquetConnector,
)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _parse_pred(sql: str):
    """Parse a bare WHERE expression into a sqlglot expression."""
    stmt = sqlglot.parse_one(f"SELECT 1 WHERE {sql}")
    return stmt.args["where"].this


# ──────────────────────────────────────────────────────────────────────────────
# PostgresConnector
# ──────────────────────────────────────────────────────────────────────────────

class TestPostgresConnector:
    @pytest.fixture()
    def conn(self, orders_df):
        c = PostgresConnector()
        c.set_mock("orders", orders_df)
        return c

    def test_fetch_all_rows(self, conn, orders_df):
        result = conn.fetch("orders", [], [])
        assert len(result.data) == len(orders_df)

    def test_equality_pushdown(self, conn):
        pred = _parse_pred("status = 'shipped'")
        result = conn.fetch("orders", ["id", "status"], [pred])
        assert all(result.data["status"] == "shipped")

    def test_gt_pushdown(self, conn):
        pred = _parse_pred("total > 100")
        result = conn.fetch("orders", ["id", "total"], [pred])
        assert all(result.data["total"] > 100)

    def test_column_projection(self, conn):
        result = conn.fetch("orders", ["id", "total"], [])
        assert list(result.data.columns) == ["id", "total"]

    def test_limit(self, conn):
        result = conn.fetch("orders", [], [], limit=2)
        assert len(result.data) == 2

    def test_estimate_rows(self, conn, orders_df):
        assert conn.estimate_rows("orders", []) == len(orders_df)

    def test_in_predicate(self, conn):
        pred = _parse_pred("status IN ('shipped', 'pending')")
        result = conn.fetch("orders", ["status"], [pred])
        assert set(result.data["status"]).issubset({"shipped", "pending"})

    def test_combined_predicates(self, conn):
        p1 = _parse_pred("status = 'shipped'")
        p2 = _parse_pred("total > 50")
        result = conn.fetch("orders", ["id", "status", "total"], [p1, p2])
        assert all(result.data["status"] == "shipped")
        assert all(result.data["total"] > 50)


# ──────────────────────────────────────────────────────────────────────────────
# MongoDBConnector
# ──────────────────────────────────────────────────────────────────────────────

class TestMongoDBConnector:
    @pytest.fixture()
    def conn(self, users_df):
        c = MongoDBConnector()
        c.set_mock("users", users_df)
        return c

    def test_fetch_all(self, conn, users_df):
        result = conn.fetch("users", [], [])
        assert len(result.data) == len(users_df)

    def test_equality_filter(self, conn):
        pred = _parse_pred("country = 'US'")
        result = conn.fetch("users", [], [pred])
        assert all(result.data["country"] == "US")
        assert len(result.data) == 2

    def test_projection(self, conn):
        result = conn.fetch("users", ["id", "name"], [])
        assert set(result.data.columns) == {"id", "name"}

    def test_gte_filter(self, conn):
        pred = _parse_pred("age >= 35")
        result = conn.fetch("users", ["name", "age"], [pred])
        assert all(result.data["age"] >= 35)

    def test_source_label(self, conn):
        result = conn.fetch("users", [], [])
        assert result.source == "mongodb"


# ──────────────────────────────────────────────────────────────────────────────
# S3ParquetConnector
# ──────────────────────────────────────────────────────────────────────────────

class TestS3ParquetConnector:
    @pytest.fixture()
    def conn(self, events_df):
        c = S3ParquetConnector()
        c.set_mock("events", events_df)
        return c

    def test_fetch_all(self, conn, events_df):
        result = conn.fetch("events", [], [])
        assert len(result.data) == len(events_df)

    def test_event_type_filter(self, conn):
        pred = _parse_pred("event_type = 'purchase'")
        result = conn.fetch("events", [], [pred])
        assert all(result.data["event_type"] == "purchase")
        assert len(result.data) == 3

    def test_limit(self, conn):
        result = conn.fetch("events", [], [], limit=2)
        assert len(result.data) == 2

    def test_rows_scanned_vs_returned(self, conn, events_df):
        pred = _parse_pred("event_type = 'purchase'")
        result = conn.fetch("events", [], [pred])
        assert result.rows_scanned == len(events_df)
        assert result.rows_returned < result.rows_scanned


# ──────────────────────────────────────────────────────────────────────────────
# RestApiConnector
# ──────────────────────────────────────────────────────────────────────────────

class TestRestApiConnector:
    @pytest.fixture()
    def conn(self, products_df):
        c = RestApiConnector()
        c.set_mock("products", products_df)
        return c

    def test_fetch_all(self, conn, products_df):
        result = conn.fetch("products", [], [])
        assert len(result.data) == len(products_df)

    def test_category_filter(self, conn):
        pred = _parse_pred("category = 'electronics'")
        result = conn.fetch("products", [], [pred])
        assert all(result.data["category"] == "electronics")

    def test_price_filter(self, conn):
        pred = _parse_pred("price < 25")
        result = conn.fetch("products", ["name", "price"], [pred])
        assert all(result.data["price"] < 25)

    def test_source_label(self, conn):
        result = conn.fetch("products", [], [])
        assert result.source == "rest_api"
