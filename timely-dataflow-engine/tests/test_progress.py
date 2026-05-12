"""Progress tracker + frontier."""

from __future__ import annotations

import pytest

from timely.progress.frontier import Frontier
from timely.progress.tracker import InvariantViolation, ProgressTracker
from timely.timestamp.ts import Timestamp


def test_initial_state():
    pt = ProgressTracker()
    assert pt.total_pending() == 0
    assert pt.active_pointstamps() == []


def test_update_records_count():
    pt = ProgressTracker()
    pt.update("opA", Timestamp(0, 0), +1)
    assert pt.count("opA", Timestamp(0, 0)) == 1
    pt.update("opA", Timestamp(0, 0), -1)
    assert pt.count("opA", Timestamp(0, 0)) == 0


def test_completion_removes_entry():
    pt = ProgressTracker()
    pt.update("op", Timestamp(0, 0), +1)
    pt.update("op", Timestamp(0, 0), -1)
    assert ("op", Timestamp(0, 0)) not in pt.counts


def test_invariant_negative_raises():
    pt = ProgressTracker()
    with pytest.raises(InvariantViolation):
        pt.update("op", Timestamp(0, 0), -1)


def test_is_complete_at_when_empty():
    pt = ProgressTracker()
    assert pt.is_complete_at("op", Timestamp(5, 5))


def test_is_complete_at_with_active():
    pt = ProgressTracker()
    pt.update("op", Timestamp(0, 0), +1)
    assert not pt.is_complete_at("op", Timestamp(0, 0))


def test_frontier_basic():
    pt = ProgressTracker()
    pt.update("opA", Timestamp(0, 0), +1)
    pt.update("opA", Timestamp(0, 1), +1)
    f = Frontier(tracker=pt)
    ac = f.at_operator("opA")
    # Frontier = minimal element = (0, 0)
    assert Timestamp(0, 0) in ac
    assert Timestamp(0, 1) not in ac


def test_frontier_passed():
    pt = ProgressTracker()
    pt.update("op", Timestamp(1, 0), +1)
    f = Frontier(tracker=pt)
    # Frontier has not passed (0, 0) — but (1, 0) ≤ (0, 0)? No.
    # Actually: passed((0,0)) returns False if any active ≤ (0,0).
    # Active (1, 0): is (1, 0) ≤ (0, 0)? No → no active is ≤ (0, 0) → passed.
    assert f.passed(Timestamp(0, 0))
    # passed((1, 0)) = no active ≤ (1, 0)? (1, 0) ≤ (1, 0) → yes → NOT passed.
    assert not f.passed(Timestamp(1, 0))
