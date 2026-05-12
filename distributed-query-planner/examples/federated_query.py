"""Federated query example: 3-table catalog across MongoDB, Parquet, and PostgreSQL.

Shows how to build the catalog, register stats, run the optimizer,
and print the chosen plan with cost estimates.

Run from the project root:
    python examples/federated_query.py
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dqp.catalog import Catalog, ColumnSchema, TableSchema
from dqp.cost.model import CostModel
from dqp.cost.statistics import ColumnStats, Histogram, StatsRegistry, TableStats
from dqp.engines.mongodb_engine import MongoDBEngine
from dqp.engines.postgres_engine import PostgresEngine
from dqp.logical_plan import JoinNode, PushedScanNode, plan_to_str
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

try:
    from dqp.engines.parquet_engine import ParquetEngine
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def col(name: str, table: str | None = None) -> ColumnRef:
    return ColumnRef(column=name, table=table)


def int_lit(v: int) -> Literal:
    return Literal(value=v, dtype="int")


def str_lit(v: str) -> Literal:
    return Literal(value=v, dtype="str")


def build_uniform_histogram(lo: float, hi: float, n_buckets: int = 10) -> Histogram:
    step = (hi - lo) / n_buckets
    boundaries = [lo + i * step for i in range(n_buckets + 1)]
    frequencies = [1.0 / n_buckets] * n_buckets
    return Histogram(boundaries=boundaries, frequencies=frequencies)


# ---------------------------------------------------------------------------
# Build catalog
# ---------------------------------------------------------------------------


def build_catalog() -> Catalog:
    catalog = Catalog()

    # Users table in PostgreSQL
    catalog.register_table(TableSchema(
        name="users",
        engine_name="postgres",
        columns=[
            ColumnSchema("user_id", "int", nullable=False, primary_key=True),
            ColumnSchema("name", "str", nullable=True),
            ColumnSchema("age", "int", nullable=True),
            ColumnSchema("email", "str", nullable=True),
            ColumnSchema("status", "str", nullable=True),
            ColumnSchema("deleted_at", "datetime", nullable=True),
        ],
        row_count_hint=500_000,
    ))

    # Events table in Parquet
    catalog.register_table(TableSchema(
        name="events",
        engine_name="parquet",
        columns=[
            ColumnSchema("event_id", "int", nullable=False, primary_key=True),
            ColumnSchema("user_id", "int", nullable=False),
            ColumnSchema("event_type", "str", nullable=True),
            ColumnSchema("amount", "float", nullable=True),
            ColumnSchema("ts", "datetime", nullable=True),
        ],
        row_count_hint=10_000_000,
    ))

    # Audit logs in MongoDB
    catalog.register_table(TableSchema(
        name="audit_logs",
        engine_name="mongodb",
        columns=[
            ColumnSchema("log_id", "int", nullable=False, primary_key=True),
            ColumnSchema("user_id", "int", nullable=False),
            ColumnSchema("action", "str", nullable=True),
            ColumnSchema("severity", "str", nullable=True),
            ColumnSchema("age", "int", nullable=True),
        ],
        row_count_hint=2_000_000,
    ))

    return catalog


# ---------------------------------------------------------------------------
# Build stats registry
# ---------------------------------------------------------------------------


def build_stats_registry() -> StatsRegistry:
    registry = StatsRegistry()

    # Users stats
    registry.set_table_stats(TableStats(
        table_name="users",
        row_count=500_000,
        column_stats={
            "user_id": ColumnStats("user_id", 0.0, 500_000, 1.0, 500_000.0,
                                   build_uniform_histogram(1, 500_000, 20)),
            "age": ColumnStats("age", 0.02, 80, 18.0, 99.0,
                               build_uniform_histogram(18, 99, 10)),
            "status": ColumnStats("status", 0.0, 3, None, None, None),
            "deleted_at": ColumnStats("deleted_at", 0.85, 100_000, None, None, None),
        },
    ))

    # Events stats
    registry.set_table_stats(TableStats(
        table_name="events",
        row_count=10_000_000,
        column_stats={
            "user_id": ColumnStats("user_id", 0.0, 500_000, 1.0, 500_000.0, None),
            "event_type": ColumnStats("event_type", 0.0, 20, None, None, None),
            "amount": ColumnStats("amount", 0.1, 10_000, 0.01, 9999.99,
                                  build_uniform_histogram(0, 10_000, 20)),
        },
    ))

    # Audit logs stats
    registry.set_table_stats(TableStats(
        table_name="audit_logs",
        row_count=2_000_000,
        column_stats={
            "user_id": ColumnStats("user_id", 0.0, 500_000, 1.0, 500_000.0, None),
            "severity": ColumnStats("severity", 0.0, 5, None, None, None),
            "action": ColumnStats("action", 0.0, 50, None, None, None),
            "age": ColumnStats("age", 0.0, 80, 18.0, 99.0,
                               build_uniform_histogram(18, 99, 10)),
        },
    ))

    return registry


# ---------------------------------------------------------------------------
# Main demo
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 70)
    print("Distributed Query Planner — Federated Query Example")
    print("=" * 70)

    catalog = build_catalog()
    registry = build_stats_registry()

    engines: dict = {
        "postgres": PostgresEngine(conn_string=None),
        "mongodb": MongoDBEngine(db=None),
    }
    if PARQUET_AVAILABLE:
        engines["parquet"] = ParquetEngine(path="/data/events")
    else:
        # Stub so the optimizer doesn't fail; in production, pyarrow must be installed
        print("\n[Warning] pyarrow not installed — Parquet engine using Postgres as fallback]\n")
        # Re-register events as postgres for demo purposes
        catalog.register_table(TableSchema(
            name="events",
            engine_name="postgres",
            columns=[
                ColumnSchema("event_id", "int", nullable=False, primary_key=True),
                ColumnSchema("user_id", "int", nullable=False),
                ColumnSchema("event_type", "str", nullable=True),
                ColumnSchema("amount", "float", nullable=True),
            ],
        ))
        engines["postgres"] = PostgresEngine(conn_string=None)

    cost_model = CostModel(registry)
    optimizer = FederatedOptimizer(catalog=catalog, cost_model=cost_model, engines=engines)

    # ------------------------------------------------------------------
    # Query 1: Single table scan with complex predicates on 'users'
    # ------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("Query 1: Users aged 25-50 with 'active' or 'trial' status, not deleted")
    print("-" * 70)

    age_pred = BetweenPredicate(col("age"), int_lit(25), int_lit(50))
    status_pred = InPredicate(col("status"), [str_lit("active"), str_lit("trial")])
    not_deleted_pred = IsNullPredicate(col("deleted_at"))

    preds = [age_pred, status_pred, not_deleted_pred]
    plan = optimizer.optimize("users", preds, ["user_id", "name", "age", "status"])

    print("\nChosen plan:")
    print(plan_to_str(plan, indent=1))
    print("\nFull explanation:")
    print(optimizer.explain(plan))

    # Show the SQL that would be generated
    pg_engine = engines["postgres"]
    pr = pg_engine.pushdown_predicates(plan.pushed_predicates)
    if plan.pushed_predicates:
        sql = pg_engine.build_select_sql("users", pr, ["user_id", "name", "age", "status"])
        print(f"\nGenerated SQL:\n  {sql}")

    # ------------------------------------------------------------------
    # Query 2: Events with high-value transactions
    # ------------------------------------------------------------------
    if "parquet" in engines:
        print("\n" + "-" * 70)
        print("Query 2: High-value purchase events in Parquet")
        print("-" * 70)

        amount_pred = ComparisonPredicate(col("amount"), ComparisonOp.GTE, int_lit(1000))
        type_pred = ComparisonPredicate(col("event_type"), ComparisonOp.EQ, str_lit("purchase"))
        # LIKE not pushable in Parquet — will be residual
        like_pred = LikePredicate(col("event_type"), "purchase_%")

        plan2 = optimizer.optimize(
            "events",
            [amount_pred, type_pred, like_pred],
            ["event_id", "user_id", "amount"],
        )

        print("\nChosen plan:")
        print(plan_to_str(plan2, indent=1))
        print(f"\nPushed predicates:   {len(plan2.pushed_predicates)}")
        for p in plan2.pushed_predicates:
            print(f"  {p!r}")
        print(f"\nResidual predicates: {len(plan2.residual_predicates)}")
        for p in plan2.residual_predicates:
            print(f"  {p!r}")

    # ------------------------------------------------------------------
    # Query 3: Audit logs — MongoDB
    # ------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("Query 3: Critical audit events in MongoDB")
    print("-" * 70)

    severity_pred = InPredicate(col("severity"), [str_lit("critical"), str_lit("error")])
    age_filter = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(30))
    action_like = LikePredicate(col("action"), "login%")  # MongoDB CAN push this (regex)

    plan3 = optimizer.optimize(
        "audit_logs",
        [severity_pred, age_filter, action_like],
        ["log_id", "user_id", "action", "severity"],
    )

    print("\nChosen plan:")
    print(plan_to_str(plan3, indent=1))
    print(f"\nPushed predicates:   {len(plan3.pushed_predicates)}")
    for p in plan3.pushed_predicates:
        print(f"  {p!r}")

    mongo_engine = engines["mongodb"]
    if plan3.pushed_predicates:
        combined = (
            plan3.pushed_predicates[0]
            if len(plan3.pushed_predicates) == 1
            else mongo_engine.translate_predicate(
                AndPredicate(plan3.pushed_predicates)
            )
        )
        if len(plan3.pushed_predicates) == 1:
            mongo_filter = mongo_engine.translate_predicate(plan3.pushed_predicates[0])
        else:
            mongo_filter = mongo_engine.translate_predicate(AndPredicate(plan3.pushed_predicates))
        pipeline = mongo_engine.build_aggregation_pipeline(
            mongo_filter,
            project_stage={"log_id": 1, "user_id": 1, "action": 1, "severity": 1, "_id": 0},
        )
        print(f"\nMongoDB aggregation pipeline ({len(pipeline)} stages):")
        for i, stage in enumerate(pipeline):
            print(f"  {i}: {stage}")

    # ------------------------------------------------------------------
    # Query 4: Federated join — users JOIN events
    # ------------------------------------------------------------------
    print("\n" + "-" * 70)
    print("Query 4: Federated join — users (Postgres) JOIN events (Parquet/Postgres)")
    print("-" * 70)

    join_pred = ComparisonPredicate(
        ColumnRef(column="user_id", table="users"),
        ComparisonOp.EQ,
        ColumnRef(column="user_id", table="events"),
    )
    # Push age filter to users, amount filter to events
    user_age = ComparisonPredicate(col("age"), ComparisonOp.GTE, int_lit(30))
    event_amount = ComparisonPredicate(col("amount"), ComparisonOp.GTE, int_lit(500))

    join_plan = optimizer.optimize_join(
        left_table="users",
        right_table="events",
        join_pred=join_pred,
        filter_preds=[user_age, event_amount],
        columns=["user_id", "name", "amount"],
    )

    print("\nChosen join plan:")
    print(plan_to_str(join_plan, indent=1))

    print("\nFull join explanation:")
    print(optimizer.explain(join_plan))

    # ------------------------------------------------------------------
    # Summary: compare costs across configurations
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("Cost comparison for Query 1 (users scan):")
    print("=" * 70)
    ts = registry.get_table_stats("users")

    configs = [
        ("Full table scan (no pushdown)", [], [age_pred, status_pred, not_deleted_pred]),
        ("Full pushdown", [age_pred, status_pred, not_deleted_pred], []),
        ("Partial: only age", [age_pred], [status_pred, not_deleted_pred]),
        ("Partial: age + status", [age_pred, status_pred], [not_deleted_pred]),
    ]

    for desc, pushed, residual in configs:
        cost = cost_model.cost_pushed_scan("users", "postgres", pushed, residual, ts)
        print(f"\n  {desc}:")
        print(f"    {cost}")


if __name__ == "__main__":
    main()
