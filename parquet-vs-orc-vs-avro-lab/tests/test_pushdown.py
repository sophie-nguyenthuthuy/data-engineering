"""ColumnStats + predicate-pushdown tests."""

from __future__ import annotations

import pytest

from pova.pushdown import Op, Predicate, can_skip_row_group
from pova.stats.column import ColumnStats


def _stats(values):
    return ColumnStats.from_values(values)


def test_stats_from_values_basic():
    s = _stats([3, 1, 2, None])
    assert s.min == 1
    assert s.max == 3
    assert s.null_count == 1
    assert s.n_rows == 4


def test_stats_from_all_null_values():
    s = _stats([None, None])
    assert s.min is None and s.max is None
    assert s.null_count == 2


def test_stats_rejects_inconsistent_null_count():
    with pytest.raises(ValueError):
        ColumnStats(min=0, max=1, null_count=5, n_rows=3)


def test_predicate_rejects_invalid_construction():
    with pytest.raises(ValueError):
        Predicate(column="", op=Op.EQ, value=1)
    with pytest.raises(ValueError):
        Predicate(column="x", op=Op.EQ, value=None)
    with pytest.raises(ValueError):
        Predicate(column="x", op=Op.IS_NULL, value=1)


def test_pushdown_skips_when_value_out_of_range():
    p = Predicate(column="x", op=Op.EQ, value=999)
    assert can_skip_row_group(p, _stats([1, 2, 3]))


def test_pushdown_keeps_when_value_in_range():
    p = Predicate(column="x", op=Op.EQ, value=2)
    assert not can_skip_row_group(p, _stats([1, 2, 3]))


def test_pushdown_lt_predicate():
    s = _stats([5, 6, 7])
    assert can_skip_row_group(Predicate("x", Op.LT, 5), s)
    assert not can_skip_row_group(Predicate("x", Op.LT, 6), s)


def test_pushdown_gt_predicate():
    s = _stats([5, 6, 7])
    assert can_skip_row_group(Predicate("x", Op.GT, 7), s)
    assert not can_skip_row_group(Predicate("x", Op.GT, 6), s)


def test_pushdown_is_null_when_no_nulls():
    assert can_skip_row_group(Predicate("x", Op.IS_NULL), _stats([1, 2, 3]))


def test_pushdown_not_null_when_all_nulls():
    assert can_skip_row_group(Predicate("x", Op.NOT_NULL), _stats([None, None]))


def test_pushdown_ne_only_skips_when_all_equal_target():
    p = Predicate("x", Op.NE, 5)
    assert can_skip_row_group(p, _stats([5, 5, 5]))
    assert not can_skip_row_group(p, _stats([5, 6]))


def test_pushdown_empty_row_group_skips_selective_predicates():
    assert can_skip_row_group(Predicate("x", Op.EQ, 1), _stats([]))
    assert not can_skip_row_group(Predicate("x", Op.IS_NULL), _stats([]))


def test_pushdown_all_null_skips_value_predicate():
    """Stats min/max are None when every value is NULL."""
    assert can_skip_row_group(Predicate("x", Op.EQ, 1), _stats([None, None]))
