"""Tests for the predicate IR module."""
from __future__ import annotations

import pytest

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
    columns_referenced,
    conjuncts,
    negate,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def col(name: str) -> ColumnRef:
    return ColumnRef(column=name)


def int_lit(v: int) -> Literal:
    return Literal(value=v, dtype="int")


def str_lit(v: str) -> Literal:
    return Literal(value=v, dtype="str")


def eq(col_name: str, val: int) -> ComparisonPredicate:
    return ComparisonPredicate(col(col_name), ComparisonOp.EQ, int_lit(val))


def gt(col_name: str, val: int) -> ComparisonPredicate:
    return ComparisonPredicate(col(col_name), ComparisonOp.GT, int_lit(val))


# ---------------------------------------------------------------------------
# conjuncts() — flatten AND tree
# ---------------------------------------------------------------------------


class TestConjuncts:
    def test_single_predicate(self):
        p = eq("a", 1)
        assert conjuncts(p) == [p]

    def test_two_level_and(self):
        a, b = eq("a", 1), eq("b", 2)
        result = conjuncts(a & b)
        assert result == [a, b]

    def test_three_level_nested_and(self):
        a, b, c = eq("a", 1), eq("b", 2), eq("c", 3)
        nested = (a & b) & c
        result = conjuncts(nested)
        assert result == [a, b, c]

    def test_deeply_nested_and(self):
        preds = [eq(str(i), i) for i in range(6)]
        combined = preds[0]
        for p in preds[1:]:
            combined = combined & p
        result = conjuncts(combined)
        assert result == preds

    def test_or_not_flattened(self):
        a, b = eq("a", 1), eq("b", 2)
        or_pred = a | b
        result = conjuncts(or_pred)
        assert len(result) == 1
        assert result[0] is or_pred

    def test_and_containing_or(self):
        a, b, c = eq("a", 1), eq("b", 2), eq("c", 3)
        # (a AND (b OR c)) — should give [a, (b OR c)]
        result = conjuncts(a & (b | c))
        assert len(result) == 2
        assert result[0] is a
        assert isinstance(result[1], OrPredicate)


# ---------------------------------------------------------------------------
# columns_referenced()
# ---------------------------------------------------------------------------


class TestColumnsReferenced:
    def test_simple_comparison(self):
        p = eq("age", 30)
        refs = columns_referenced(p)
        assert refs == {col("age")}

    def test_between_predicate(self):
        p = BetweenPredicate(col("age"), int_lit(20), int_lit(40))
        refs = columns_referenced(p)
        assert refs == {col("age")}

    def test_and_predicate(self):
        p = eq("age", 30) & eq("name", 1)
        refs = columns_referenced(p)
        assert refs == {col("age"), col("name")}

    def test_or_predicate(self):
        p = eq("x", 1) | eq("y", 2) | eq("z", 3)
        refs = columns_referenced(p)
        assert refs == {col("x"), col("y"), col("z")}

    def test_not_predicate(self):
        p = NotPredicate(eq("status", 1))
        refs = columns_referenced(p)
        assert refs == {col("status")}

    def test_like_predicate(self):
        p = LikePredicate(col("name"), "Alice%")
        refs = columns_referenced(p)
        assert refs == {col("name")}

    def test_is_null_predicate(self):
        p = IsNullPredicate(col("deleted_at"))
        refs = columns_referenced(p)
        assert refs == {col("deleted_at")}

    def test_in_predicate(self):
        p = InPredicate(col("status"), [str_lit("active"), str_lit("pending")])
        refs = columns_referenced(p)
        assert refs == {col("status")}

    def test_nested_complex(self):
        p = (eq("a", 1) & eq("b", 2)) | eq("c", 3)
        refs = columns_referenced(p)
        assert refs == {col("a"), col("b"), col("c")}

    def test_table_qualified_column(self):
        ref = ColumnRef(column="id", table="orders")
        p = ComparisonPredicate(ref, ComparisonOp.EQ, int_lit(42))
        refs = columns_referenced(p)
        assert refs == {ref}


# ---------------------------------------------------------------------------
# Operator overloading: __and__, __or__, __invert__
# ---------------------------------------------------------------------------


class TestOperators:
    def test_and_produces_and_predicate(self):
        a, b = eq("a", 1), eq("b", 2)
        result = a & b
        assert isinstance(result, AndPredicate)
        assert a in result.predicates
        assert b in result.predicates

    def test_and_flattens_nested(self):
        a, b, c = eq("a", 1), eq("b", 2), eq("c", 3)
        result = (a & b) & c
        assert isinstance(result, AndPredicate)
        assert len(result.predicates) == 3

    def test_or_produces_or_predicate(self):
        a, b = eq("a", 1), eq("b", 2)
        result = a | b
        assert isinstance(result, OrPredicate)
        assert a in result.predicates
        assert b in result.predicates

    def test_or_flattens_nested(self):
        a, b, c = eq("a", 1), eq("b", 2), eq("c", 3)
        result = (a | b) | c
        assert isinstance(result, OrPredicate)
        assert len(result.predicates) == 3

    def test_invert_produces_not(self):
        a = eq("a", 1)
        result = ~a
        assert isinstance(result, NotPredicate)
        assert result.predicate is a

    def test_double_invert(self):
        a = eq("a", 1)
        double_neg = ~~a
        assert isinstance(double_neg, NotPredicate)
        assert isinstance(double_neg.predicate, NotPredicate)

    def test_chaining(self):
        a, b, c, d = eq("a", 1), eq("b", 2), eq("c", 3), eq("d", 4)
        result = (a & b) | (c & d)
        assert isinstance(result, OrPredicate)
        assert len(result.predicates) == 2


# ---------------------------------------------------------------------------
# negate() — De Morgan's law
# ---------------------------------------------------------------------------


class TestNegate:
    def test_negate_eq_becomes_neq(self):
        p = eq("a", 5)
        result = negate(p)
        assert isinstance(result, ComparisonPredicate)
        assert result.op == ComparisonOp.NEQ

    def test_negate_lt_becomes_gte(self):
        p = ComparisonPredicate(col("x"), ComparisonOp.LT, int_lit(10))
        result = negate(p)
        assert isinstance(result, ComparisonPredicate)
        assert result.op == ComparisonOp.GTE

    def test_negate_gt_becomes_lte(self):
        p = ComparisonPredicate(col("x"), ComparisonOp.GT, int_lit(10))
        result = negate(p)
        assert result.op == ComparisonOp.LTE

    def test_negate_lte_becomes_gt(self):
        p = ComparisonPredicate(col("x"), ComparisonOp.LTE, int_lit(10))
        result = negate(p)
        assert result.op == ComparisonOp.GT

    def test_negate_gte_becomes_lt(self):
        p = ComparisonPredicate(col("x"), ComparisonOp.GTE, int_lit(10))
        result = negate(p)
        assert result.op == ComparisonOp.LT

    def test_negate_and_demorgan(self):
        a, b = eq("a", 1), eq("b", 2)
        and_pred = a & b
        result = negate(and_pred)
        # NOT (a AND b) = (NOT a) OR (NOT b)
        assert isinstance(result, OrPredicate)
        assert len(result.predicates) == 2
        for child in result.predicates:
            assert isinstance(child, ComparisonPredicate)
            assert child.op == ComparisonOp.NEQ

    def test_negate_or_demorgan(self):
        a, b = eq("a", 1), eq("b", 2)
        or_pred = a | b
        result = negate(or_pred)
        # NOT (a OR b) = (NOT a) AND (NOT b)
        assert isinstance(result, AndPredicate)
        assert len(result.predicates) == 2

    def test_negate_not_eliminates(self):
        a = eq("a", 1)
        not_pred = NotPredicate(a)
        result = negate(not_pred)
        # NOT (NOT a) = a
        assert result is a

    def test_negate_in_predicate(self):
        p = InPredicate(col("x"), [int_lit(1), int_lit(2)])
        result = negate(p)
        assert isinstance(result, InPredicate)
        assert result.negated is True

    def test_negate_between_predicate(self):
        p = BetweenPredicate(col("x"), int_lit(0), int_lit(100))
        result = negate(p)
        assert isinstance(result, BetweenPredicate)
        assert result.negated is True

    def test_negate_like_predicate(self):
        p = LikePredicate(col("name"), "Alice%")
        result = negate(p)
        assert isinstance(result, LikePredicate)
        assert result.negated is True

    def test_negate_is_null(self):
        p = IsNullPredicate(col("deleted_at"))
        result = negate(p)
        assert isinstance(result, IsNullPredicate)
        assert result.negated is True

    def test_negate_is_not_null(self):
        p = IsNullPredicate(col("deleted_at"), negated=True)
        result = negate(p)
        assert isinstance(result, IsNullPredicate)
        assert result.negated is False

    def test_demorgan_three_way_and(self):
        a, b, c = eq("a", 1), eq("b", 2), eq("c", 3)
        and3 = AndPredicate([a, b, c])
        result = negate(and3)
        # NOT (a AND b AND c) = (NOT a) OR (NOT b) OR (NOT c)
        assert isinstance(result, OrPredicate)
        assert len(result.predicates) == 3
