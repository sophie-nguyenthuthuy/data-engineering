"""Type system + Schema."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ppc.ir.schema import Column, Schema
from ppc.ir.types import BOOLEAN, DOUBLE, INT32, INT64, STRING, TIMESTAMP, promote


def test_canonical_singletons_unique():
    assert INT32 is INT32
    assert INT32 != INT64
    assert STRING != INT64


def test_promote_widens():
    assert promote(INT32, INT32) == INT32
    assert promote(INT32, INT64) == INT64
    assert promote(INT64, INT32) == INT64
    assert promote(INT32, DOUBLE) == DOUBLE
    assert promote(DOUBLE, DOUBLE) == DOUBLE
    assert promote(STRING, STRING) == STRING


def test_promote_propagates_nullable():
    nullable_i = INT32.with_nullable(True)
    not_null_i = INT32.with_nullable(False)
    assert promote(nullable_i, not_null_i).nullable
    assert not promote(not_null_i, not_null_i).nullable


def test_promote_incompatible_raises():
    with pytest.raises(TypeError):
        promote(STRING, INT64)
    with pytest.raises(TypeError):
        promote(BOOLEAN, DOUBLE)


def test_schema_lookup():
    s = Schema.of(Column("a", INT64), Column("b", DOUBLE), rows=100)
    assert s["a"].dtype == INT64
    assert s["b"].dtype == DOUBLE
    assert s.index("b") == 1
    with pytest.raises(KeyError):
        _ = s["missing"]


def test_schema_project():
    s = Schema.of(Column("a", INT64), Column("b", DOUBLE), Column("c", STRING), rows=100)
    p = s.project(["a", "c"])
    assert p.names == ("a", "c")
    assert p.rows == 100


def test_schema_union_dedups():
    a = Schema.of(Column("k", INT64), Column("x", DOUBLE), rows=10)
    b = Schema.of(Column("k", INT64), Column("y", DOUBLE), rows=20)
    u = a.union(b)
    assert u.names == ("k", "x", "y")
    # Row estimate is cross-product upper bound when no refinement
    assert u.rows == 200


def test_schema_row_width():
    s = Schema.of(Column("a", INT64), Column("b", DOUBLE))  # 8 + 8 = 16
    assert s.row_width == 16


def test_schema_bytes_estimate_with_unknown_rows():
    import math

    s = Schema.of(Column("a", INT64))
    assert math.isnan(s.bytes_estimate())


@given(
    cols=st.lists(
        st.tuples(st.text(min_size=1, max_size=10, alphabet="abcdefg"),
                  st.sampled_from([INT32, INT64, DOUBLE, STRING, BOOLEAN, TIMESTAMP])),
        min_size=1, max_size=8, unique_by=lambda x: x[0],
    ),
    rows=st.integers(min_value=0, max_value=10**12),
)
def test_schema_property_row_width_consistent(cols, rows):
    s = Schema.of(*(Column(n, t) for n, t in cols), rows=rows)
    assert s.row_width == sum(c.dtype.byte_width for c in s.columns)
    assert s.bytes_estimate() == rows * s.row_width
