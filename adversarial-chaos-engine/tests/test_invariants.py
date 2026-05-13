"""Catalog + builtin invariant tests."""

from __future__ import annotations

import pytest

from ace.invariants.catalog import Catalog, default_catalog
from ace.invariants.checks import (
    column_no_nulls,
    column_value_range,
    distinct_count_preserved,
    monotone_increasing,
    row_count_preserved,
    sum_invariant,
)

# ---------------------------------------------------------------- catalog


def test_catalog_isolation_between_instances():
    cat_a = Catalog()
    cat_b = Catalog()

    @cat_a.invariant(row_count_preserved=True)
    def f1(frame):
        return frame

    assert f1.__name__ in cat_a.names()
    assert f1.__name__ not in cat_b.names()


def test_catalog_clear_resets_state():
    cat = Catalog()

    @cat.invariant(row_count_preserved=True)
    def g(frame):
        return frame

    cat.clear()
    assert cat.names() == []


def test_catalog_referenced_columns_aggregates_across_specs():
    cat = Catalog()

    @cat.invariant(sum_invariant=["amount"], no_nulls=["name"])
    def h(frame):
        return frame

    assert cat.referenced_columns() == {"amount", "name"}


def test_catalog_specs_for_unknown_returns_empty():
    assert Catalog().specs_for("nobody") == []


def test_decorator_does_not_change_function_behaviour():
    cat = Catalog()

    @cat.invariant(row_count_preserved=True)
    def h(frame):
        return [{"out": True}]

    assert h([{"a": 1}]) == [{"out": True}]


def test_default_catalog_is_singleton_module_scoped():
    a = default_catalog()
    b = default_catalog()
    assert a is b


# -------------------------------------------------------- builtin checks


def test_row_count_preserved_basic():
    assert row_count_preserved([{}, {}], [{}, {}])
    assert not row_count_preserved([{}, {}], [{}])


def test_sum_invariant_passes_when_equal():
    check = sum_invariant("a")
    assert check([{"a": 1}, {"a": 2}], [{"a": 3}])


def test_sum_invariant_fails_when_changed():
    check = sum_invariant("a")
    assert not check([{"a": 1}, {"a": 2}], [{"a": 4}])


def test_sum_invariant_treats_non_numeric_as_zero():
    check = sum_invariant("a")
    assert check([{"a": 1}, {"a": "oops"}], [{"a": 1}])


def test_sum_invariant_fails_on_nan():
    check = sum_invariant("a")
    assert not check([{"a": float("nan")}], [{"a": 0}])


def test_column_no_nulls():
    check = column_no_nulls("name")
    assert check([], [{"name": "x"}])
    assert not check([], [{"name": None}])


def test_column_value_range_inclusive():
    check = column_value_range("v", 0.0, 10.0)
    assert check([], [{"v": 0}, {"v": 10}])
    assert not check([], [{"v": -1}])
    assert not check([], [{"v": 11}])


def test_column_value_range_rejects_nan():
    check = column_value_range("v", 0.0, 1.0)
    assert not check([], [{"v": float("nan")}])


def test_column_value_range_rejects_bad_bounds():
    with pytest.raises(ValueError):
        column_value_range("v", 5.0, 1.0)


def test_monotone_increasing_passes_on_sorted():
    check = monotone_increasing("v")
    assert check([], [{"v": 1}, {"v": 2}, {"v": 2}, {"v": 3}])


def test_monotone_increasing_fails_on_decreasing():
    check = monotone_increasing("v")
    assert not check([], [{"v": 3}, {"v": 1}])


def test_distinct_count_preserved():
    check = distinct_count_preserved("k")
    assert check([{"k": 1}, {"k": 2}], [{"k": 2}, {"k": 1}])
    assert not check([{"k": 1}, {"k": 2}], [{"k": 1}])
