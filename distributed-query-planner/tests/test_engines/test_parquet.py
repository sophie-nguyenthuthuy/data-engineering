"""Tests for ParquetEngine predicate translation.

PyArrow expressions don't support __eq__ comparison with Python objects in
the usual sense, so we test by checking the *string representation* or
by applying the expression to an in-memory table and verifying row counts.
"""
from __future__ import annotations

import pytest

pyarrow = pytest.importorskip("pyarrow", reason="pyarrow not installed")
pa = pyarrow
import pyarrow as pa
import pyarrow.dataset as ds

from dqp.engines.parquet_engine import ParquetEngine
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
    NotPredicate,
    OrPredicate,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def col(name: str) -> ColumnRef:
    return ColumnRef(column=name)


def int_lit(v: int) -> Literal:
    return Literal(value=v, dtype="int")


def str_lit(v: str) -> Literal:
    return Literal(value=v, dtype="str")


@pytest.fixture()
def engine() -> ParquetEngine:
    return ParquetEngine(path="/tmp/nonexistent.parquet")


def apply_filter(expr, table: pa.Table) -> pa.Table:
    """Apply a PyArrow dataset expression to an in-memory table."""
    return table.filter(expr)


def make_age_table() -> pa.Table:
    """Create an in-memory table with an 'age' column for filter testing."""
    return pa.table({"age": pa.array(list(range(100))), "name": pa.array([f"user{i}" for i in range(100)])})


def make_nullable_table() -> pa.Table:
    ages = [10, 20, None, 40, None, 60]
    return pa.table({"age": pa.array(ages, type=pa.int64())})


# ---------------------------------------------------------------------------
# Comparison predicates
# ---------------------------------------------------------------------------


class TestTranslateComparison:
    def test_eq(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(42))
        expr = engine.translate_predicate(pred)
        assert expr is not None
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 1
        assert result["age"][0].as_py() == 42

    def test_neq(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.NEQ, int_lit(42))
        expr = engine.translate_predicate(pred)
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 99

    def test_lt(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(10))
        expr = engine.translate_predicate(pred)
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 10  # 0..9

    def test_lte(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.LTE, int_lit(10))
        expr = engine.translate_predicate(pred)
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 11  # 0..10

    def test_gt(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(90))
        expr = engine.translate_predicate(pred)
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 9  # 91..99

    def test_gte(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GTE, int_lit(90))
        expr = engine.translate_predicate(pred)
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 10  # 90..99


# ---------------------------------------------------------------------------
# IN predicate
# ---------------------------------------------------------------------------


class TestTranslateIn:
    def test_in_positive(self, engine):
        pred = InPredicate(col("age"), [int_lit(5), int_lit(10), int_lit(15)])
        expr = engine.translate_predicate(pred)
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 3

    def test_not_in(self, engine):
        # NOT IN (5, 10, 15) → 97 rows
        pred = InPredicate(col("age"), [int_lit(5), int_lit(10), int_lit(15)], negated=True)
        expr = engine.translate_predicate(pred)
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 97

    def test_empty_in_returns_expression(self, engine):
        pred = InPredicate(col("age"), [])
        expr = engine.translate_predicate(pred)
        # Should return an expression (even if it matches nothing)
        assert expr is not None


# ---------------------------------------------------------------------------
# BETWEEN predicate
# ---------------------------------------------------------------------------


class TestTranslateBetween:
    def test_between(self, engine):
        pred = BetweenPredicate(col("age"), int_lit(10), int_lit(20))
        expr = engine.translate_predicate(pred)
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 11  # 10..20 inclusive

    def test_not_between(self, engine):
        pred = BetweenPredicate(col("age"), int_lit(10), int_lit(20), negated=True)
        expr = engine.translate_predicate(pred)
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 89  # everything outside [10,20]


# ---------------------------------------------------------------------------
# IS NULL
# ---------------------------------------------------------------------------


class TestTranslateIsNull:
    def test_is_null(self, engine):
        pred = IsNullPredicate(col("age"))
        expr = engine.translate_predicate(pred)
        table = make_nullable_table()
        result = apply_filter(expr, table)
        assert len(result) == 2  # two None values

    def test_is_not_null(self, engine):
        pred = IsNullPredicate(col("age"), negated=True)
        expr = engine.translate_predicate(pred)
        table = make_nullable_table()
        result = apply_filter(expr, table)
        assert len(result) == 4


# ---------------------------------------------------------------------------
# LIKE — not supported, should return None
# ---------------------------------------------------------------------------


class TestLikeNotPushable:
    def test_like_returns_none(self, engine):
        pred = LikePredicate(col("name"), "Alice%")
        result = engine.translate_predicate(pred)
        assert result is None

    def test_not_like_returns_none(self, engine):
        pred = LikePredicate(col("name"), "Alice%", negated=True)
        result = engine.translate_predicate(pred)
        assert result is None

    def test_like_not_in_capabilities(self, engine):
        from dqp.engines.base import EngineCapability
        assert EngineCapability.LIKE not in engine.capabilities

    def test_can_push_like_returns_false(self, engine):
        pred = LikePredicate(col("name"), "Alice%")
        assert engine.can_push(pred) is False


# ---------------------------------------------------------------------------
# AND combines with &
# ---------------------------------------------------------------------------


class TestTranslateAnd:
    def test_and_two_predicates(self, engine):
        a = ComparisonPredicate(col("age"), ComparisonOp.GTE, int_lit(10))
        b = ComparisonPredicate(col("age"), ComparisonOp.LTE, int_lit(20))
        pred = AndPredicate([a, b])
        expr = engine.translate_predicate(pred)
        assert expr is not None
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 11  # 10..20

    def test_and_three_predicates(self, engine):
        a = ComparisonPredicate(col("age"), ComparisonOp.GTE, int_lit(0))
        b = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(50))
        c = ComparisonPredicate(col("age"), ComparisonOp.NEQ, int_lit(25))
        pred = AndPredicate([a, b, c])
        expr = engine.translate_predicate(pred)
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 49  # 0..49 minus 25

    def test_and_with_like_skips_like(self, engine):
        # AND containing LIKE — the LIKE part should be skipped (returns None),
        # but the valid parts should still be translated
        a = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(50))
        like = LikePredicate(col("name"), "%foo%")
        pred = AndPredicate([a, like])
        expr = engine.translate_predicate(pred)
        # Should still get an expression from 'a', even though LIKE is skipped
        assert expr is not None

    def test_and_all_like_returns_none(self, engine):
        like1 = LikePredicate(col("name"), "foo%")
        like2 = LikePredicate(col("name"), "bar%")
        pred = AndPredicate([like1, like2])
        expr = engine.translate_predicate(pred)
        assert expr is None


# ---------------------------------------------------------------------------
# Build filter expression
# ---------------------------------------------------------------------------


class TestBuildFilterExpression:
    def test_empty_list_returns_none(self, engine):
        result = engine.build_filter_expression([])
        assert result is None

    def test_single_predicate(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(50))
        expr = engine.build_filter_expression([pred])
        assert expr is not None

    def test_multiple_predicates_combined(self, engine):
        a = ComparisonPredicate(col("age"), ComparisonOp.GTE, int_lit(10))
        b = ComparisonPredicate(col("age"), ComparisonOp.LTE, int_lit(20))
        expr = engine.build_filter_expression([a, b])
        table = make_age_table()
        result = apply_filter(expr, table)
        assert len(result) == 11
