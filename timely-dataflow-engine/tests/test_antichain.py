"""Antichain operations."""

from __future__ import annotations

from timely.timestamp.antichain import Antichain
from timely.timestamp.ts import Timestamp


def test_empty():
    a = Antichain()
    assert len(a) == 0
    assert Timestamp(0, 0) not in a


def test_insert_basic():
    a = Antichain()
    a.insert(Timestamp(1, 0))
    a.insert(Timestamp(0, 1))   # incomparable → both kept
    assert len(a) == 2


def test_insert_dominated_skipped():
    """Inserting a dominated element is a no-op."""
    a = Antichain()
    a.insert(Timestamp(0, 0))
    a.insert(Timestamp(5, 5))      # dominated by (0,0)
    assert len(a) == 1
    assert Timestamp(0, 0) in a
    assert Timestamp(5, 5) not in a


def test_insert_dominates_existing_removes_old():
    a = Antichain()
    a.insert(Timestamp(5, 5))
    a.insert(Timestamp(0, 0))      # strictly less than (5,5)
    assert len(a) == 1
    assert Timestamp(0, 0) in a
    assert Timestamp(5, 5) not in a


def test_remove():
    a = Antichain()
    a.insert(Timestamp(0, 0))
    assert a.remove(Timestamp(0, 0))
    assert len(a) == 0


def test_dominates():
    a = Antichain()
    a.insert(Timestamp(0, 0))
    assert a.dominates(Timestamp(5, 5))
    assert a.dominates(Timestamp(0, 0))
    a.remove(Timestamp(0, 0))
    a.insert(Timestamp(3, 3))
    assert not a.dominates(Timestamp(2, 2))


def test_iter_sorted():
    a = Antichain()
    for t in [Timestamp(1, 0), Timestamp(0, 1), Timestamp(2, 2)]:
        a.insert(t)
    lst = list(a)
    # Must be deterministically ordered
    assert lst == sorted(lst, key=lambda t: (t.epoch, t.iteration))
