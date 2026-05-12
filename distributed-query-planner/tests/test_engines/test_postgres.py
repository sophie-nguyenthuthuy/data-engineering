"""Tests for PostgresEngine SQL generation."""
from __future__ import annotations

import datetime

import pytest

from dqp.engines.base import PushdownResult
from dqp.engines.postgres_engine import PostgresEngine, format_value
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


def float_lit(v: float) -> Literal:
    return Literal(value=v, dtype="float")


def bool_lit(v: bool) -> Literal:
    return Literal(value=v, dtype="bool")


def date_lit(v: datetime.date) -> Literal:
    return Literal(value=v, dtype="date")


def datetime_lit(v: datetime.datetime) -> Literal:
    return Literal(value=v, dtype="datetime")


def null_lit() -> Literal:
    return Literal(value=None, dtype="null")


@pytest.fixture()
def engine() -> PostgresEngine:
    return PostgresEngine(conn_string=None)


# ---------------------------------------------------------------------------
# format_value
# ---------------------------------------------------------------------------


class TestFormatValue:
    def test_int(self):
        assert format_value(int_lit(42)) == "42"

    def test_negative_int(self):
        assert format_value(int_lit(-10)) == "-10"

    def test_float(self):
        result = format_value(float_lit(3.14))
        assert "3.14" in result

    def test_string_quoted(self):
        assert format_value(str_lit("hello")) == "'hello'"

    def test_string_with_single_quote_escaped(self):
        result = format_value(str_lit("O'Brien"))
        assert result == "'O''Brien'"

    def test_bool_true(self):
        assert format_value(bool_lit(True)) == "TRUE"

    def test_bool_false(self):
        assert format_value(bool_lit(False)) == "FALSE"

    def test_null(self):
        assert format_value(null_lit()) == "NULL"

    def test_date(self):
        d = datetime.date(2024, 1, 15)
        result = format_value(date_lit(d))
        assert result == "DATE '2024-01-15'"

    def test_datetime(self):
        dt = datetime.datetime(2024, 1, 15, 12, 30, 0)
        result = format_value(datetime_lit(dt))
        assert "TIMESTAMP" in result
        assert "2024-01-15" in result


# ---------------------------------------------------------------------------
# Individual predicate SQL fragments
# ---------------------------------------------------------------------------


class TestTranslateComparison:
    def test_eq(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(30))
        sql = engine.translate_predicate(pred)
        assert sql == '"age" = 30'

    def test_neq(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.NEQ, int_lit(30))
        sql = engine.translate_predicate(pred)
        assert sql == '"age" <> 30'

    def test_lt(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(30))
        assert engine.translate_predicate(pred) == '"age" < 30'

    def test_lte(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.LTE, int_lit(30))
        assert engine.translate_predicate(pred) == '"age" <= 30'

    def test_gt(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(30))
        assert engine.translate_predicate(pred) == '"age" > 30'

    def test_gte(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GTE, int_lit(30))
        assert engine.translate_predicate(pred) == '"age" >= 30'

    def test_string_value_quoted(self, engine):
        pred = ComparisonPredicate(col("status"), ComparisonOp.EQ, str_lit("active"))
        sql = engine.translate_predicate(pred)
        assert sql == '"status" = \'active\''

    def test_column_with_special_chars_quoted(self, engine):
        pred = ComparisonPredicate(
            ColumnRef(column='my "col"'), ComparisonOp.EQ, int_lit(1)
        )
        sql = engine.translate_predicate(pred)
        # Double quotes inside identifier should be doubled
        assert '"my ""col"""' in sql


class TestTranslateIn:
    def test_in_positive(self, engine):
        pred = InPredicate(col("status"), [str_lit("active"), str_lit("pending")])
        sql = engine.translate_predicate(pred)
        assert sql == '"status" IN (\'active\', \'pending\')'

    def test_not_in(self, engine):
        pred = InPredicate(col("id"), [int_lit(1), int_lit(2)], negated=True)
        sql = engine.translate_predicate(pred)
        assert sql == '"id" NOT IN (1, 2)'

    def test_in_single_value(self, engine):
        pred = InPredicate(col("x"), [int_lit(99)])
        sql = engine.translate_predicate(pred)
        assert sql == '"x" IN (99)'


class TestTranslateBetween:
    def test_between(self, engine):
        pred = BetweenPredicate(col("age"), int_lit(18), int_lit(65))
        sql = engine.translate_predicate(pred)
        assert sql == '"age" BETWEEN 18 AND 65'

    def test_not_between(self, engine):
        pred = BetweenPredicate(col("age"), int_lit(18), int_lit(65), negated=True)
        sql = engine.translate_predicate(pred)
        assert sql == '"age" NOT BETWEEN 18 AND 65'

    def test_between_with_date(self, engine):
        lo = date_lit(datetime.date(2024, 1, 1))
        hi = date_lit(datetime.date(2024, 12, 31))
        pred = BetweenPredicate(col("created_at"), lo, hi)
        sql = engine.translate_predicate(pred)
        assert "BETWEEN" in sql
        assert "DATE '2024-01-01'" in sql
        assert "DATE '2024-12-31'" in sql


class TestTranslateLike:
    def test_like(self, engine):
        pred = LikePredicate(col("name"), "Alice%")
        sql = engine.translate_predicate(pred)
        assert sql == '"name" LIKE \'Alice%\''

    def test_not_like(self, engine):
        pred = LikePredicate(col("name"), "Alice%", negated=True)
        sql = engine.translate_predicate(pred)
        assert sql == '"name" NOT LIKE \'Alice%\''

    def test_like_with_single_quote_in_pattern(self, engine):
        pred = LikePredicate(col("name"), "O'Brien%")
        sql = engine.translate_predicate(pred)
        assert "O''Brien" in sql


class TestTranslateIsNull:
    def test_is_null(self, engine):
        pred = IsNullPredicate(col("deleted_at"))
        sql = engine.translate_predicate(pred)
        assert sql == '"deleted_at" IS NULL'

    def test_is_not_null(self, engine):
        pred = IsNullPredicate(col("deleted_at"), negated=True)
        sql = engine.translate_predicate(pred)
        assert sql == '"deleted_at" IS NOT NULL'


class TestTranslateCompound:
    def test_and(self, engine):
        a = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        b = IsNullPredicate(col("deleted_at"), negated=True)
        pred = AndPredicate([a, b])
        sql = engine.translate_predicate(pred)
        assert sql.startswith("(")
        assert "AND" in sql
        assert '"age" > 18' in sql
        assert '"deleted_at" IS NOT NULL' in sql

    def test_or(self, engine):
        a = ComparisonPredicate(col("status"), ComparisonOp.EQ, str_lit("active"))
        b = ComparisonPredicate(col("status"), ComparisonOp.EQ, str_lit("pending"))
        pred = OrPredicate([a, b])
        sql = engine.translate_predicate(pred)
        assert "OR" in sql
        assert sql.startswith("(") and sql.endswith(")")

    def test_not(self, engine):
        inner = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(18))
        pred = NotPredicate(inner)
        sql = engine.translate_predicate(pred)
        assert sql.startswith("NOT (")

    def test_nested_and_or(self, engine):
        a = ComparisonPredicate(col("a"), ComparisonOp.GT, int_lit(1))
        b = ComparisonPredicate(col("b"), ComparisonOp.LT, int_lit(10))
        c = ComparisonPredicate(col("c"), ComparisonOp.EQ, str_lit("x"))
        pred = AndPredicate([OrPredicate([a, b]), c])
        sql = engine.translate_predicate(pred)
        assert "OR" in sql
        assert "AND" in sql


# ---------------------------------------------------------------------------
# Full SELECT statement building
# ---------------------------------------------------------------------------


class TestBuildSelectSQL:
    def test_simple_select_all(self, engine):
        result = PushdownResult(pushed=[], residual=[], native_filter=None)
        sql = engine.build_select_sql("orders", result, [], schema="public")
        assert sql == 'SELECT * FROM "public"."orders"'

    def test_select_with_columns(self, engine):
        result = PushdownResult(pushed=[], residual=[], native_filter=None)
        sql = engine.build_select_sql("orders", result, ["id", "amount"], schema="public")
        assert '"id"' in sql
        assert '"amount"' in sql

    def test_select_with_where_clause(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        where = engine.translate_predicate(pred)
        result = PushdownResult(pushed=[pred], residual=[], native_filter=where)
        sql = engine.build_select_sql("users", result, ["id", "name"])
        assert "WHERE" in sql
        assert '"age" > 18' in sql

    def test_select_custom_schema(self, engine):
        result = PushdownResult(pushed=[], residual=[], native_filter=None)
        sql = engine.build_select_sql("orders", result, [], schema="analytics")
        assert '"analytics"."orders"' in sql

    def test_select_no_where_when_no_native_filter(self, engine):
        result = PushdownResult(pushed=[], residual=[], native_filter=None)
        sql = engine.build_select_sql("orders", result, [])
        assert "WHERE" not in sql


# ---------------------------------------------------------------------------
# Partial index hint matching
# ---------------------------------------------------------------------------


class TestPartialIndexHint:
    def _indexes(self):
        return [
            {
                "name": "idx_orders_active",
                "predicate": "status = 'active'",
                "columns": ["status", "created_at"],
            },
            {
                "name": "idx_users_adult",
                "predicate": "age >= 18",
                "columns": ["age"],
            },
        ]

    def test_matching_index_found(self, engine):
        pred = ComparisonPredicate(col("status"), ComparisonOp.EQ, str_lit("active"))
        hint = engine.partial_index_hint(pred, self._indexes())
        assert hint == "idx_orders_active"

    def test_no_matching_index(self, engine):
        pred = ComparisonPredicate(col("amount"), ComparisonOp.GT, int_lit(1000))
        hint = engine.partial_index_hint(pred, self._indexes())
        assert hint is None

    def test_empty_index_list(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(30))
        hint = engine.partial_index_hint(pred, [])
        assert hint is None

    def test_age_index_found(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GTE, int_lit(18))
        hint = engine.partial_index_hint(pred, self._indexes())
        assert hint == "idx_users_adult"

    def test_all_capabilities_present(self, engine):
        from dqp.engines.base import EngineCapability
        caps = engine.capabilities
        for cap in EngineCapability:
            assert cap in caps
