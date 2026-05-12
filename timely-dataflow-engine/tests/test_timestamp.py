"""Timestamp partial order + lattice."""

from __future__ import annotations

from timely.timestamp.ts import Timestamp, comparable


def test_equal():
    assert Timestamp(0, 0) == Timestamp(0, 0)
    assert Timestamp(1, 2) != Timestamp(1, 3)


def test_partial_order_le():
    assert Timestamp(0, 0) <= Timestamp(1, 0)
    assert Timestamp(0, 0) <= Timestamp(0, 1)
    assert Timestamp(0, 0) <= Timestamp(1, 1)


def test_strict_lt():
    assert Timestamp(0, 0) < Timestamp(1, 0)
    assert not (Timestamp(0, 0) < Timestamp(0, 0))


def test_incomparable():
    """(1, 0) and (0, 1) are not comparable."""
    a = Timestamp(1, 0)
    b = Timestamp(0, 1)
    assert not (a <= b)
    assert not (b <= a)
    assert not comparable(a, b)


def test_lattice_join():
    """Component-wise max."""
    assert Timestamp(1, 2).join(Timestamp(3, 0)) == Timestamp(3, 2)


def test_lattice_meet():
    """Component-wise min."""
    assert Timestamp(1, 2).meet(Timestamp(3, 0)) == Timestamp(1, 0)


def test_next_iter():
    assert Timestamp(2, 5).next_iter() == Timestamp(2, 6)


def test_next_epoch_resets_iter():
    assert Timestamp(2, 5).next_epoch() == Timestamp(3, 0)


def test_hashable():
    s = {Timestamp(0, 0), Timestamp(0, 0), Timestamp(1, 0)}
    assert len(s) == 2
