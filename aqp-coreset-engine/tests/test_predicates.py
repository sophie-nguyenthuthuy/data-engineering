"""Predicate combinator tests."""

from __future__ import annotations

import pytest

from aqp.queries.predicates import (
    always_true,
    and_,
    box_pred,
    eq_pred,
    range_pred,
)


def test_always_true_selects_everything():
    p = always_true()
    assert p((1.0,)) and p((42.0, -1.0))


def test_eq_pred_basic():
    p = eq_pred(0, 3.0)
    assert p((3.0, 99.0))
    assert not p((4.0, 99.0))


def test_eq_pred_rejects_negative_col():
    with pytest.raises(ValueError):
        eq_pred(-1, 0.0)


def test_range_pred_inclusive():
    p = range_pred(1, 0.0, 1.0)
    assert p((0.0, 0.0))
    assert p((0.0, 1.0))
    assert p((0.0, 0.5))
    assert not p((0.0, 1.01))


def test_range_pred_rejects_inverted_bounds():
    with pytest.raises(ValueError):
        range_pred(0, 1.0, 0.0)


def test_box_pred_requires_all_cols_in_range():
    p = box_pred({0: (0.0, 1.0), 1: (10.0, 20.0)})
    assert p((0.5, 15.0))
    assert not p((1.5, 15.0))
    assert not p((0.5, 25.0))


def test_box_pred_rejects_empty():
    with pytest.raises(ValueError):
        box_pred({})


def test_and_of_zero_preds_is_true():
    p = and_()
    assert p((0.0,))


def test_and_of_preds_short_circuits_false():
    p = and_(eq_pred(0, 1.0), range_pred(1, 0.0, 1.0))
    assert p((1.0, 0.5))
    assert not p((0.0, 0.5))
    assert not p((1.0, 5.0))
