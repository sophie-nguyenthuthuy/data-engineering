"""Expression IR."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from ppc.ir.expr import AND, NOT, OR, BinaryOp, column, lit
from ppc.ir.types import BOOLEAN, DOUBLE, INT32, INT64, STRING


def test_literal_inferred_types():
    assert lit(1).dtype == INT64
    assert lit(1.0).dtype == DOUBLE
    assert lit("x").dtype == STRING
    assert lit(True).dtype == BOOLEAN


def test_column_ref_referenced_columns():
    c = column("a", INT64)
    assert c.referenced_columns() == frozenset({"a"})


def test_binop_compare_dtype_is_boolean():
    e = column("a", INT64) > lit(5)
    assert e.dtype == BOOLEAN


def test_binop_arith_promotes():
    e = column("a", INT32) + column("b", DOUBLE)
    assert e.dtype == DOUBLE


def test_referenced_columns_union():
    e = (column("a", INT64) + column("b", INT64)) > column("c", INT64)
    assert e.referenced_columns() == frozenset({"a", "b", "c"})


def test_constant_folding_for_literal_only():
    e = BinaryOp(op="+", left=lit(1), right=lit(2))
    assert e.evaluate_const() == 3


def test_constant_folding_returns_none_for_columns():
    e = column("a", INT64) + lit(1)
    assert e.evaluate_const() is None


def test_logical_ops_construct():
    a = column("a", INT64) > lit(1)
    b = column("b", INT64) < lit(5)
    e = AND(a, b)
    assert isinstance(e, BinaryOp)
    assert e.op == "AND"
    assert e.referenced_columns() == frozenset({"a", "b"})

    o = OR(a, b)
    assert o.op == "OR"

    n = NOT(a)
    assert n.op == "NOT"


def test_eq_method_returns_binop_not_python_eq():
    """Don't break dataclass hashing by overriding __eq__."""
    c = column("a", INT64)
    e = c.eq(lit(1))
    assert isinstance(e, BinaryOp)
    assert e.op == "="


@given(st.integers(min_value=-100, max_value=100), st.integers(min_value=-100, max_value=100))
def test_arith_round_trips_via_evaluate_const(a, b):
    e = BinaryOp(op="+", left=lit(a), right=lit(b))
    assert e.evaluate_const() == a + b


def test_binop_hashable_for_memo_use():
    """BinaryOp must be hashable so the Memo can use it as a key component."""
    e = column("a", INT64) > lit(5)
    s = {e, e}
    assert len(s) == 1
