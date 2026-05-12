"""Tests for the FederatedOptimizer."""
from __future__ import annotations

import pytest

from dqp.catalog import Catalog, ColumnSchema, TableSchema
from dqp.cost.model import CostModel, PlanCost
from dqp.cost.statistics import ColumnStats, Histogram, StatsRegistry, TableStats
from dqp.engines.mongodb_engine import MongoDBEngine
from dqp.engines.parquet_engine import ParquetEngine
from dqp.engines.postgres_engine import PostgresEngine
from dqp.logical_plan import FilterNode, JoinNode, PushedScanNode
from dqp.optimizer import FederatedOptimizer
from dqp.predicate import (
    AndPredicate,
    BetweenPredicate,
    ColumnRef,
    ComparisonOp,
    ComparisonPredicate,
    InPredicate,
    IsNullPredicate,
    LikePredicate,
    Literal,
    OrPredicate,
)

pyarrow_available = pytest.importorskip


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def col(name: str) -> ColumnRef:
    return ColumnRef(column=name)


def int_lit(v: int) -> Literal:
    return Literal(value=v, dtype="int")


def str_lit(v: str) -> Literal:
    return Literal(value=v, dtype="str")


def make_stats(table_name: str, row_count: int = 100_000, age_ndv: int = 100) -> TableStats:
    boundaries = [float(i * 10) for i in range(11)]
    frequencies = [0.1] * 10
    h = Histogram(boundaries=boundaries, frequencies=frequencies)
    age = ColumnStats("age", null_fraction=0.0, distinct_count=age_ndv,
                      min_value=0.0, max_value=100.0, histogram=h)
    status = ColumnStats("status", null_fraction=0.0, distinct_count=4,
                         min_value=None, max_value=None, histogram=None)
    user_id = ColumnStats("user_id", null_fraction=0.0, distinct_count=row_count,
                          min_value=1.0, max_value=float(row_count), histogram=None)
    order_id = ColumnStats("order_id", null_fraction=0.0, distinct_count=row_count,
                           min_value=1.0, max_value=float(row_count), histogram=None)
    return TableStats(
        table_name=table_name,
        row_count=row_count,
        column_stats={"age": age, "status": status, "user_id": user_id, "order_id": order_id},
    )


# ---------------------------------------------------------------------------
# Fixture: a catalog with three tables on different engines
# ---------------------------------------------------------------------------


@pytest.fixture()
def catalog() -> Catalog:
    c = Catalog()
    c.register_table(TableSchema(
        name="users",
        engine_name="postgres",
        columns=[
            ColumnSchema("user_id", "int", nullable=False, primary_key=True),
            ColumnSchema("age", "int", nullable=True),
            ColumnSchema("status", "str", nullable=True),
        ],
        row_count_hint=100_000,
    ))
    c.register_table(TableSchema(
        name="events",
        engine_name="parquet",
        columns=[
            ColumnSchema("user_id", "int", nullable=False),
            ColumnSchema("event_type", "str", nullable=True),
            ColumnSchema("age", "int", nullable=True),
        ],
        row_count_hint=1_000_000,
    ))
    c.register_table(TableSchema(
        name="logs",
        engine_name="mongodb",
        columns=[
            ColumnSchema("order_id", "int", nullable=False),
            ColumnSchema("status", "str", nullable=True),
            ColumnSchema("age", "int", nullable=True),
        ],
        row_count_hint=500_000,
    ))
    return c


@pytest.fixture()
def registry(catalog: Catalog) -> StatsRegistry:
    reg = StatsRegistry()
    reg.set_table_stats(make_stats("users", row_count=100_000))
    reg.set_table_stats(make_stats("events", row_count=1_000_000))
    reg.set_table_stats(make_stats("logs", row_count=500_000))
    return reg


@pytest.fixture()
def engines() -> dict:
    return {
        "postgres": PostgresEngine(conn_string=None),
        "parquet": ParquetEngine(path="/tmp/events.parquet"),
        "mongodb": MongoDBEngine(db=None),
    }


@pytest.fixture()
def optimizer(catalog, registry, engines) -> FederatedOptimizer:
    cost_model = CostModel(registry)
    return FederatedOptimizer(catalog=catalog, cost_model=cost_model, engines=engines)


# ---------------------------------------------------------------------------
# Basic optimization: pushed vs not-pushed cost
# ---------------------------------------------------------------------------


class TestOptimizeBasic:
    def test_returns_pushed_scan_node(self, optimizer):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        node = optimizer.optimize("users", [pred], ["user_id", "age"])
        assert isinstance(node, PushedScanNode)
        assert node.table_name == "users"
        assert node.engine_name == "postgres"

    def test_pushable_predicate_is_pushed(self, optimizer):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        node = optimizer.optimize("users", [pred], ["user_id", "age"])
        assert pred in node.pushed_predicates
        assert len(node.residual_predicates) == 0

    def test_no_predicates_yields_full_scan(self, optimizer):
        node = optimizer.optimize("users", [], ["user_id"])
        assert isinstance(node, PushedScanNode)
        assert len(node.pushed_predicates) == 0
        assert len(node.residual_predicates) == 0

    def test_pushed_reduces_rows_out(self, optimizer, registry):
        # With a highly selective predicate, rows_out should be much less than row_count
        # age = 42 has selectivity 1/100 → expected ~1000 rows
        pred = ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(42))
        node = optimizer.optimize("users", [pred], ["user_id"])
        ts = registry.get_table_stats("users")
        cost_model = CostModel(registry)
        cost = cost_model.cost_pushed_scan(
            "users", "postgres", node.pushed_predicates, node.residual_predicates, ts
        )
        assert cost.rows_out < 5_000  # much less than 100k

    def test_like_not_pushable_to_parquet(self, optimizer):
        # LIKE is not in ParquetEngine capabilities — should end up as residual
        pred = LikePredicate(col("age"), "5%")
        node = optimizer.optimize("events", [pred], ["user_id"])
        # The predicate must be in residual (parquet can't push LIKE)
        assert pred in node.residual_predicates


# ---------------------------------------------------------------------------
# Partial pushdown: AND predicates
# ---------------------------------------------------------------------------


class TestPartialPushdown:
    def test_partial_pushdown_separates_pushable(self, optimizer):
        # Parquet can push COMPARISON but not LIKE
        age_pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        like_pred = LikePredicate(col("age"), "2%")
        node = optimizer.optimize("events", [age_pred, like_pred], ["user_id"])
        assert age_pred in node.pushed_predicates
        assert like_pred in node.residual_predicates

    def test_multiple_pushable_predicates(self, optimizer):
        a = ComparisonPredicate(col("age"), ComparisonOp.GTE, int_lit(18))
        b = ComparisonPredicate(col("age"), ComparisonOp.LTE, int_lit(65))
        node = optimizer.optimize("users", [a, b], ["user_id", "age"])
        # Both should be pushed (Postgres can push all)
        assert a in node.pushed_predicates
        assert b in node.pushed_predicates

    def test_or_predicate_optimization(self, optimizer):
        # OR containing pushable predicates — optimizer should handle it
        a = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(18))
        b = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(65))
        or_pred = OrPredicate([a, b])
        node = optimizer.optimize("users", [or_pred], ["user_id"])
        assert isinstance(node, PushedScanNode)

    def test_between_predicate_pushed(self, optimizer):
        pred = BetweenPredicate(col("age"), int_lit(18), int_lit(65))
        node = optimizer.optimize("logs", [pred], ["order_id"])
        # MongoDB supports BETWEEN
        assert pred in node.pushed_predicates

    def test_in_predicate_pushed(self, optimizer):
        pred = InPredicate(col("status"), [str_lit("active"), str_lit("pending")])
        node = optimizer.optimize("logs", [pred], ["order_id"])
        assert pred in node.pushed_predicates

    def test_is_null_pushed(self, optimizer):
        pred = IsNullPredicate(col("age"))
        node = optimizer.optimize("users", [pred], ["user_id"])
        assert pred in node.pushed_predicates


# ---------------------------------------------------------------------------
# Join optimization
# ---------------------------------------------------------------------------


class TestJoinOptimization:
    def test_optimize_join_returns_plan_node(self, optimizer):
        join_pred = ComparisonPredicate(
            ColumnRef(column="user_id", table="users"),
            ComparisonOp.EQ,
            ColumnRef(column="user_id", table="events"),
        )
        node = optimizer.optimize_join(
            left_table="users",
            right_table="events",
            join_pred=join_pred,
            filter_preds=[],
            columns=["user_id", "age"],
        )
        assert node is not None

    def test_join_contains_scan_nodes(self, optimizer):
        join_pred = ComparisonPredicate(
            ColumnRef(column="user_id", table="users"),
            ComparisonOp.EQ,
            ColumnRef(column="user_id", table="events"),
        )
        node = optimizer.optimize_join(
            left_table="users",
            right_table="events",
            join_pred=join_pred,
            filter_preds=[],
            columns=["user_id"],
        )
        # Should be a JoinNode with two PushedScanNode children
        assert isinstance(node, JoinNode)
        assert isinstance(node.left, PushedScanNode)
        assert isinstance(node.right, PushedScanNode)

    def test_join_per_table_filter_pushdown(self, optimizer):
        join_pred = ComparisonPredicate(
            ColumnRef(column="user_id", table="users"),
            ComparisonOp.EQ,
            ColumnRef(column="user_id", table="events"),
        )
        # Use a table-qualified ColumnRef so the optimizer can unambiguously
        # route this predicate to the left (users) side of the join.
        age_col = ColumnRef(column="age", table="users")
        age_pred = ComparisonPredicate(age_col, ComparisonOp.GT, int_lit(18))
        node = optimizer.optimize_join(
            left_table="users",
            right_table="events",
            join_pred=join_pred,
            filter_preds=[age_pred],
            columns=["user_id", "age"],
        )
        assert isinstance(node, JoinNode)
        left_scan = node.left
        assert isinstance(left_scan, PushedScanNode)
        assert age_pred in left_scan.pushed_predicates


# ---------------------------------------------------------------------------
# Explain
# ---------------------------------------------------------------------------


class TestExplain:
    def test_explain_returns_string(self, optimizer):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        node = optimizer.optimize("users", [pred], ["user_id", "age"])
        explanation = optimizer.explain(node)
        assert isinstance(explanation, str)
        assert len(explanation) > 0

    def test_explain_contains_table_name(self, optimizer):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        node = optimizer.optimize("users", [pred], ["user_id", "age"])
        explanation = optimizer.explain(node)
        assert "users" in explanation

    def test_explain_contains_engine_name(self, optimizer):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        node = optimizer.optimize("users", [pred], ["user_id", "age"])
        explanation = optimizer.explain(node)
        assert "postgres" in explanation

    def test_explain_shows_cost(self, optimizer):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        node = optimizer.optimize("users", [pred], ["user_id", "age"])
        explanation = optimizer.explain(node)
        assert "cost" in explanation.lower() or "Cost" in explanation


# ---------------------------------------------------------------------------
# Engine not registered raises helpful error
# ---------------------------------------------------------------------------


class TestEngineNotRegistered:
    def test_missing_engine_raises_key_error(self, catalog, registry):
        cost_model = CostModel(registry)
        optimizer = FederatedOptimizer(
            catalog=catalog,
            cost_model=cost_model,
            engines={"postgres": PostgresEngine()},  # missing parquet and mongodb
        )
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        with pytest.raises(KeyError, match="parquet"):
            optimizer.optimize("events", [pred], ["user_id"])
