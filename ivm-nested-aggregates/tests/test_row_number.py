"""ROW_NUMBER IVM."""

from __future__ import annotations

import random


def test_insert_in_order(row_number):
    deltas = row_number.insert("p", 10, "r1")
    assert deltas == [("r1", 1)]
    deltas = row_number.insert("p", 20, "r2")
    assert deltas == [("r2", 2)]
    deltas = row_number.insert("p", 30, "r3")
    assert deltas == [("r3", 3)]


def test_insert_in_middle_shifts_suffix(row_number):
    row_number.insert("p", 10, "r1")
    row_number.insert("p", 30, "r3")
    deltas = row_number.insert("p", 20, "r2")
    # Affected suffix: r2 at rank 2, r3 at rank 3
    d = dict(deltas)
    assert d["r2"] == 2
    assert d["r3"] == 3
    assert "r1" not in d   # not affected


def test_partition_isolation(row_number):
    row_number.insert("p1", 10, "x")
    row_number.insert("p2", 5, "y")
    assert row_number.rank("p1", 10, "x") == 1
    assert row_number.rank("p2", 5, "y") == 1


def test_delete_shifts_suffix(row_number):
    for t, rid in [(10, "a"), (20, "b"), (30, "c"), (40, "d")]:
        row_number.insert("p", t, rid)
    deltas = row_number.delete("p", 20, "b")
    d = dict(deltas)
    assert d["c"] == 2
    assert d["d"] == 3


def test_delete_missing_returns_empty(row_number):
    assert row_number.delete("p", 1, "x") == []


def test_matches_ground_truth_random():
    """Compare against full-recompute on random workloads."""
    from ivm.window.row_number import RowNumberIVM
    rn = RowNumberIVM()
    rng = random.Random(0)
    rows: list[tuple[float, str]] = []
    for i in range(200):
        t = rng.uniform(0, 100)
        rid = f"r{i}"
        rn.insert("p", t, rid)
        rows.append((t, rid))
    rows.sort()
    for expected_rank, (t, rid) in enumerate(rows, start=1):
        assert rn.rank("p", t, rid) == expected_rank
