"""Vector-clock partial-order tests."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cfs.clock.vector_clock import (
    bump,
    concurrent,
    dominates,
    equal,
    lt,
    pointwise_max,
)


def test_dominates_strictly_greater():
    assert dominates({"a": 2}, {"a": 1})
    assert not dominates({"a": 1}, {"a": 2})


def test_dominates_treats_missing_as_zero():
    assert dominates({"a": 1}, {})
    assert dominates({"a": 1, "b": 0}, {"a": 1})


def test_equal_reflexive():
    assert equal({"a": 1, "b": 2}, {"b": 2, "a": 1})


def test_equal_missing_zero():
    assert equal({}, {"a": 0})


def test_lt_strict():
    assert lt({"a": 1}, {"a": 2})
    assert not lt({"a": 1}, {"a": 1})
    assert not lt({"a": 2}, {"a": 1})


def test_concurrent_basic():
    assert concurrent({"a": 1, "b": 0}, {"a": 0, "b": 1})


def test_concurrent_false_on_dominates():
    assert not concurrent({"a": 2}, {"a": 1})


def test_pointwise_max_zero_args_is_empty():
    assert pointwise_max() == {}


def test_pointwise_max_takes_componentwise_max():
    a = {"x": 1, "y": 3}
    b = {"x": 5, "y": 2, "z": 4}
    assert pointwise_max(a, b) == {"x": 5, "y": 3, "z": 4}


def test_pointwise_max_idempotent():
    a = {"x": 1, "y": 2}
    assert pointwise_max(a, a) == a


def test_bump_increments_named_component():
    assert bump({"a": 1}, "a") == {"a": 2}
    assert bump({}, "a") == {"a": 1}


def test_bump_does_not_mutate_input():
    base = {"a": 1}
    out = bump(base, "b")
    assert base == {"a": 1}
    assert out == {"a": 1, "b": 1}


def test_bump_rejects_empty_component():
    with pytest.raises(ValueError):
        bump({}, "")


def test_validate_rejects_negative_counter():
    with pytest.raises(ValueError):
        dominates({"a": -1}, {})


# -------------------------------------------------------- Hypothesis lattice


_small_clocks = st.dictionaries(
    keys=st.sampled_from(["a", "b", "c", "d"]),
    values=st.integers(0, 5),
    min_size=0,
    max_size=4,
)


@settings(max_examples=80, deadline=None)
@given(_small_clocks)
def test_property_dominates_reflexive(c):
    assert dominates(c, c)


@settings(max_examples=80, deadline=None)
@given(_small_clocks, _small_clocks)
def test_property_dominates_antisymmetric_on_equality(a, b):
    if dominates(a, b) and dominates(b, a):
        assert equal(a, b)


@settings(max_examples=60, deadline=None)
@given(_small_clocks, _small_clocks, _small_clocks)
def test_property_dominates_transitive(a, b, c):
    if dominates(a, b) and dominates(b, c):
        assert dominates(a, c)


@settings(max_examples=60, deadline=None)
@given(_small_clocks, _small_clocks)
def test_property_pointwise_max_dominates_both(a, b):
    j = pointwise_max(a, b)
    assert dominates(j, a) and dominates(j, b)


@settings(max_examples=60, deadline=None)
@given(_small_clocks, _small_clocks)
def test_property_pointwise_max_commutative(a, b):
    assert pointwise_max(a, b) == pointwise_max(b, a)


@settings(max_examples=40, deadline=None)
@given(_small_clocks, _small_clocks, _small_clocks)
def test_property_pointwise_max_associative(a, b, c):
    left = pointwise_max(pointwise_max(a, b), c)
    right = pointwise_max(a, pointwise_max(b, c))
    assert left == right
