"""Tests for WindowOperator — tumbling, sliding, and partition windows."""
import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ivm import IVMEngine, TumblingWindow, SlidingWindow, PartitionWindow
import ivm.aggregates as agg


# ---------------------------------------------------------------------------
# Tumbling window
# ---------------------------------------------------------------------------

def test_tumbling_single_window():
    e = IVMEngine()
    src = e.source("s")
    v = src.window(TumblingWindow(size_ms=10_000),
                   aggregates={"count": agg.Count(), "total": agg.Sum("val")})
    e.register_view("v", v)

    e.ingest("s", {"val": 5}, timestamp=1_000)
    e.ingest("s", {"val": 3}, timestamp=5_000)

    rows = e.query("v")
    assert len(rows) == 1
    assert rows[0]["count"] == 2
    assert rows[0]["total"] == 8
    assert rows[0]["window_start"] == 0
    assert rows[0]["window_end"] == 9_999


def test_tumbling_two_windows():
    e = IVMEngine()
    src = e.source("s")
    v = src.window(TumblingWindow(size_ms=10_000), aggregates={"count": agg.Count()})
    e.register_view("v", v)

    e.ingest("s", {"x": 1}, timestamp=5_000)   # window 0
    e.ingest("s", {"x": 2}, timestamp=15_000)  # window 1
    e.ingest("s", {"x": 3}, timestamp=25_000)  # window 2

    rows = sorted(e.query("v"), key=lambda r: r["window_start"])
    assert len(rows) == 3
    assert all(r["count"] == 1 for r in rows)


def test_tumbling_retraction():
    e = IVMEngine()
    src = e.source("s")
    v = src.window(TumblingWindow(size_ms=10_000), aggregates={"count": agg.Count()})
    e.register_view("v", v)

    e.ingest("s",  {"x": 1}, timestamp=3_000)
    e.ingest("s",  {"x": 2}, timestamp=7_000)
    e.retract("s", {"x": 1}, timestamp=3_000)

    rows = e.query("v")
    assert len(rows) == 1
    assert rows[0]["count"] == 1


def test_tumbling_window_empty_after_full_retraction():
    e = IVMEngine()
    src = e.source("s")
    v = src.window(TumblingWindow(size_ms=10_000), aggregates={"count": agg.Count()})
    e.register_view("v", v)

    e.ingest("s",  {"x": 1}, timestamp=1_000)
    e.retract("s", {"x": 1}, timestamp=1_000)

    assert e.query("v") == []


# ---------------------------------------------------------------------------
# Sliding window
# ---------------------------------------------------------------------------

def test_sliding_membership():
    """Record at t=15000 with size=30000, step=10000 belongs to 3 windows.

    Windows are aligned at multiples of step_ms (including negative starts):
      wid=-1: [-10000, 19999]  ← contains 15000
      wid= 0: [     0, 29999]  ← contains 15000
      wid= 1: [ 10000, 39999]  ← contains 15000
    """
    e = IVMEngine()
    src = e.source("s")
    spec = SlidingWindow(size_ms=30_000, step_ms=10_000)
    v = src.window(spec, aggregates={"count": agg.Count()})
    e.register_view("v", v)

    e.ingest("s", {"x": 1}, timestamp=15_000)

    rows = e.query("v")
    assert len(rows) == 3
    assert all(r["count"] == 1 for r in rows)
    # Every window that covers t=15000 must have 15000 within its bounds
    for row in rows:
        assert row["window_start"] <= 15_000 <= row["window_end"]


def test_sliding_retraction():
    e = IVMEngine()
    src = e.source("s")
    spec = SlidingWindow(size_ms=30_000, step_ms=10_000)
    v = src.window(spec, aggregates={"count": agg.Count()})
    e.register_view("v", v)

    e.ingest("s",  {"x": 1}, timestamp=5_000)
    e.ingest("s",  {"x": 2}, timestamp=5_000)
    e.retract("s", {"x": 1}, timestamp=5_000)

    # Only x=2 remains
    for row in e.query("v"):
        assert row["count"] == 1


# ---------------------------------------------------------------------------
# Partition window (ROW_NUMBER)
# ---------------------------------------------------------------------------

def test_row_number_basic():
    e = IVMEngine()
    src = e.source("s")
    spec = PartitionWindow(partition_by=["user"], order_by=[("score", "desc")])
    v = src.window(spec, rank_fns={"rn": "row_number"})
    e.register_view("v", v)

    e.ingest("s", {"user": "alice", "score": 100}, timestamp=1)
    e.ingest("s", {"user": "alice", "score": 50},  timestamp=2)
    e.ingest("s", {"user": "alice", "score": 75},  timestamp=3)

    rows = sorted(e.query("v"), key=lambda r: r["rn"])
    scores = [r["score"] for r in rows]
    assert scores == [100, 75, 50]   # descending
    assert [r["rn"] for r in rows] == [1, 2, 3]


def test_row_number_multiple_partitions():
    e = IVMEngine()
    src = e.source("s")
    spec = PartitionWindow(partition_by=["dept"], order_by=[("salary", "asc")])
    v = src.window(spec, rank_fns={"rn": "row_number"})
    e.register_view("v", v)

    data = [
        {"dept": "eng",  "name": "a", "salary": 100},
        {"dept": "eng",  "name": "b", "salary": 90},
        {"dept": "sales","name": "c", "salary": 80},
        {"dept": "sales","name": "d", "salary": 95},
    ]
    for r in data:
        e.ingest("s", r, timestamp=1)

    by_dept = {}
    for r in e.query("v"):
        by_dept.setdefault(r["dept"], []).append((r["rn"], r["salary"]))

    # eng: salary asc → 90 is rn=1, 100 is rn=2
    eng = sorted(by_dept["eng"])
    assert eng[0] == (1, 90)
    assert eng[1] == (2, 100)

    # sales: salary asc → 80 is rn=1, 95 is rn=2
    sales = sorted(by_dept["sales"])
    assert sales[0] == (1, 80)
    assert sales[1] == (2, 95)


def test_row_number_insert_shifts_ranks():
    e = IVMEngine()
    src = e.source("s")
    spec = PartitionWindow(partition_by=["g"], order_by=[("v", "asc")])
    view = src.window(spec, rank_fns={"rn": "row_number"})
    e.register_view("v", view)

    e.ingest("s", {"g": "x", "v": 10}, timestamp=1)
    e.ingest("s", {"g": "x", "v": 30}, timestamp=2)

    # Insert a value between them — 30 should become rn=3
    e.ingest("s", {"g": "x", "v": 20}, timestamp=3)

    rows = sorted(e.query("v"), key=lambda r: r["rn"])
    assert [r["v"] for r in rows] == [10, 20, 30]
    assert [r["rn"] for r in rows] == [1, 2, 3]


def test_row_number_retraction_shifts_ranks():
    e = IVMEngine()
    src = e.source("s")
    spec = PartitionWindow(partition_by=["g"], order_by=[("v", "asc")])
    view = src.window(spec, rank_fns={"rn": "row_number"})
    e.register_view("v", view)

    for val in [10, 20, 30]:
        e.ingest("s", {"g": "x", "v": val}, timestamp=1)

    # Retract v=20 — v=30 should become rn=2
    e.retract("s", {"g": "x", "v": 20}, timestamp=2)

    rows = sorted(e.query("v"), key=lambda r: r["rn"])
    assert [r["v"] for r in rows] == [10, 30]
    assert [r["rn"] for r in rows] == [1, 2]
