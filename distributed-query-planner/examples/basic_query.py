"""Basic query example: create predicates and translate to MongoDB, Parquet, and Postgres.

Run from the project root:
    python examples/basic_query.py
"""
from __future__ import annotations

import sys
import os

# Ensure the src directory is on the path when running directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

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
    columns_referenced,
    conjuncts,
    negate,
)
from dqp.engines.mongodb_engine import MongoDBEngine
from dqp.engines.postgres_engine import PostgresEngine

try:
    from dqp.engines.parquet_engine import ParquetEngine
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False


def main() -> None:
    print("=" * 60)
    print("Distributed Query Planner — Basic Query Example")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Build predicates
    # ------------------------------------------------------------------
    age_col = ColumnRef(column="age")
    status_col = ColumnRef(column="status")
    name_col = ColumnRef(column="name")
    deleted_at_col = ColumnRef(column="deleted_at")

    # age > 18
    age_gt = ComparisonPredicate(age_col, ComparisonOp.GT, Literal(18, "int"))

    # age BETWEEN 18 AND 65
    age_between = BetweenPredicate(age_col, Literal(18, "int"), Literal(65, "int"))

    # status IN ('active', 'pending')
    status_in = InPredicate(
        status_col,
        [Literal("active", "str"), Literal("pending", "str")],
    )

    # name LIKE 'Alice%'
    name_like = LikePredicate(name_col, "Alice%")

    # deleted_at IS NULL
    not_deleted = IsNullPredicate(deleted_at_col)

    # Complex: (age > 18 AND status IN ('active', 'pending')) OR name LIKE 'Alice%'
    complex_pred = (age_gt & status_in) | name_like

    print("\n--- Predicates ---")
    print(f"  age_gt:       {age_gt!r}")
    print(f"  age_between:  {age_between!r}")
    print(f"  status_in:    {status_in!r}")
    print(f"  name_like:    {name_like!r}")
    print(f"  not_deleted:  {not_deleted!r}")
    print(f"  complex:      {complex_pred!r}")

    print("\n--- Columns referenced in complex predicate ---")
    refs = columns_referenced(complex_pred)
    for ref in sorted(refs, key=str):
        print(f"  {ref}")

    print("\n--- Conjuncts of (age_gt AND status_in AND not_deleted) ---")
    combined = age_gt & status_in & not_deleted
    for c in conjuncts(combined):
        print(f"  {c!r}")

    print("\n--- De Morgan: negate(age_gt AND status_in) ---")
    negated = negate(age_gt & status_in)
    print(f"  {negated!r}")

    # ------------------------------------------------------------------
    # MongoDB translation
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("MongoDB Translation")
    print("=" * 60)
    mongo_engine = MongoDBEngine(db=None)

    print("\n  age_gt:")
    print(f"    {mongo_engine.translate_predicate(age_gt)}")

    print("\n  age_between:")
    print(f"    {mongo_engine.translate_predicate(age_between)}")

    print("\n  status_in:")
    print(f"    {mongo_engine.translate_predicate(status_in)}")

    print("\n  name_like:")
    print(f"    {mongo_engine.translate_predicate(name_like)}")

    print("\n  not_deleted:")
    print(f"    {mongo_engine.translate_predicate(not_deleted)}")

    print("\n  complex (age_gt AND status_in):")
    and_part = age_gt & status_in
    mongo_filter = mongo_engine.translate_predicate(and_part)
    print(f"    {mongo_filter}")

    print("\n  Full aggregation pipeline:")
    pipeline = mongo_engine.build_aggregation_pipeline(
        mongo_filter,
        project_stage={"name": 1, "age": 1, "status": 1, "_id": 0},
    )
    for i, stage in enumerate(pipeline):
        print(f"    Stage {i}: {stage}")

    # ------------------------------------------------------------------
    # Parquet translation
    # ------------------------------------------------------------------
    if PARQUET_AVAILABLE:
        print("\n" + "=" * 60)
        print("Parquet / PyArrow Translation")
        print("=" * 60)
        parquet_engine = ParquetEngine(path="/tmp/users.parquet")

        for pred_name, pred in [
            ("age_gt", age_gt),
            ("age_between", age_between),
            ("status_in", status_in),
            ("not_deleted (IS NULL)", not_deleted),
            ("name_like (NOT pushable)", name_like),
        ]:
            expr = parquet_engine.translate_predicate(pred)
            print(f"\n  {pred_name}:")
            if expr is not None:
                print(f"    {expr}")
            else:
                print("    <not pushable — will remain as residual filter>")

        pushdown = parquet_engine.pushdown_predicates([age_gt, age_between, name_like])
        print(f"\n  Pushdown result for [age_gt, age_between, name_like]:")
        print(f"    Pushed ({len(pushdown.pushed)}):   {[repr(p) for p in pushdown.pushed]}")
        print(f"    Residual ({len(pushdown.residual)}): {[repr(p) for p in pushdown.residual]}")
    else:
        print("\n[Parquet engine skipped — pyarrow not installed]")

    # ------------------------------------------------------------------
    # Postgres translation
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("PostgreSQL Translation")
    print("=" * 60)
    pg_engine = PostgresEngine(conn_string=None)

    for pred_name, pred in [
        ("age_gt", age_gt),
        ("age_between", age_between),
        ("status_in", status_in),
        ("name_like", name_like),
        ("not_deleted (IS NULL)", not_deleted),
        ("complex AND", age_gt & status_in & not_deleted),
        ("complex OR", age_gt | status_in),
        ("NOT age_gt", ~age_gt),
    ]:
        sql = pg_engine.translate_predicate(pred)
        print(f"\n  {pred_name}:")
        print(f"    {sql}")

    # Full SELECT
    print("\n  Full SELECT for (age > 18 AND status IN ('active','pending')):")
    and_pred = age_gt & status_in
    pushdown_pg = pg_engine.pushdown_predicates([and_pred])
    sql = pg_engine.build_select_sql("users", pushdown_pg, ["id", "name", "age"])
    print(f"    {sql}")

    # Partial index hint
    indexes = [
        {"name": "idx_users_active", "predicate": "status = 'active'", "columns": ["status"]},
        {"name": "idx_users_adult", "predicate": "age > 18", "columns": ["age"]},
    ]
    hint = pg_engine.partial_index_hint(age_gt, indexes)
    print(f"\n  Partial index hint for age_gt: {hint!r}")


if __name__ == "__main__":
    main()
