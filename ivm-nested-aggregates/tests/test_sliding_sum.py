"""Sliding SUM/AVG."""

from __future__ import annotations


def test_basic_sum(sliding):
    for t, v in [(1, 10), (2, 20), (3, 30)]:
        sliding.insert("p", t, v)
    # Window of size 5 covers everything so far
    assert sliding.sliding_sum("p", 3) == 60.0
    assert sliding.sliding_avg("p", 3) == 20.0


def test_window_limits_history(sliding):
    """window_size=5; after inserting 7 items, sum at index 6 covers last 5."""
    for t, v in [(i, i + 1) for i in range(7)]:
        sliding.insert("p", float(t), float(v))
    # Window [2..6] (5 items): values 3+4+5+6+7 = 25
    assert sliding.sliding_sum("p", 6.0) == 25.0


def test_avg(sliding):
    for t, v in [(1, 10), (2, 20), (3, 30)]:
        sliding.insert("p", t, v)
    assert sliding.sliding_avg("p", 3) == 20.0


def test_insert_in_middle_updates_prefix(sliding):
    sliding.insert("p", 10, 100)
    sliding.insert("p", 30, 300)
    sliding.insert("p", 20, 200)        # in the middle
    # All three within window 5; sum at t=30 = 600
    assert sliding.sliding_sum("p", 30) == 600.0


def test_delete(sliding):
    for t, v in [(1, 10), (2, 20), (3, 30)]:
        sliding.insert("p", t, v)
    sliding.delete("p", 2, 20)
    # Now [1,3] → sum at t=3 = 40
    assert sliding.sliding_sum("p", 3) == 40.0


def test_missing_returns_none(sliding):
    assert sliding.sliding_sum("p", 99) is None
