"""Tests for GroupByOperator — aggregate correctness and retraction safety."""
import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ivm import IVMEngine
import ivm.aggregates as agg


def make_engine():
    e = IVMEngine()
    src = e.source("events")
    return e, src


# ---------------------------------------------------------------------------
# COUNT
# ---------------------------------------------------------------------------

def test_count_basic():
    e, src = make_engine()
    view = src.group_by(["color"], {"n": agg.Count()})
    e.register_view("v", view)

    e.ingest("events", {"color": "red"},   timestamp=1)
    e.ingest("events", {"color": "red"},   timestamp=2)
    e.ingest("events", {"color": "blue"},  timestamp=3)

    rows = {r["color"]: r["n"] for r in e.query("v")}
    assert rows == {"red": 2, "blue": 1}


def test_count_retraction():
    e, src = make_engine()
    view = src.group_by(["color"], {"n": agg.Count()})
    e.register_view("v", view)

    e.ingest("events",  {"color": "red"}, timestamp=1)
    e.ingest("events",  {"color": "red"}, timestamp=2)
    e.retract("events", {"color": "red"}, timestamp=3)

    rows = {r["color"]: r["n"] for r in e.query("v")}
    assert rows == {"red": 1}


def test_count_group_disappears_on_full_retraction():
    e, src = make_engine()
    view = src.group_by(["k"], {"n": agg.Count()})
    e.register_view("v", view)

    e.ingest("events",  {"k": "x"}, timestamp=1)
    e.retract("events", {"k": "x"}, timestamp=2)

    assert e.query("v") == []


# ---------------------------------------------------------------------------
# SUM
# ---------------------------------------------------------------------------

def test_sum_basic():
    e, src = make_engine()
    view = src.group_by(["cat"], {"total": agg.Sum("val")})
    e.register_view("v", view)

    for val in [10, 20, 30]:
        e.ingest("events", {"cat": "A", "val": val}, timestamp=1)

    rows = {r["cat"]: r["total"] for r in e.query("v")}
    assert rows["A"] == 60


def test_sum_retraction():
    e, src = make_engine()
    view = src.group_by(["cat"], {"total": agg.Sum("val")})
    e.register_view("v", view)

    e.ingest("events",  {"cat": "A", "val": 100}, timestamp=1)
    e.ingest("events",  {"cat": "A", "val": 50},  timestamp=2)
    e.retract("events", {"cat": "A", "val": 100}, timestamp=3)

    rows = {r["cat"]: r["total"] for r in e.query("v")}
    assert rows["A"] == 50


# ---------------------------------------------------------------------------
# AVG
# ---------------------------------------------------------------------------

def test_avg_basic():
    e, src = make_engine()
    view = src.group_by(["g"], {"avg": agg.Avg("v")})
    e.register_view("v", view)

    e.ingest("events", {"g": "x", "v": 10}, timestamp=1)
    e.ingest("events", {"g": "x", "v": 20}, timestamp=2)
    e.ingest("events", {"g": "x", "v": 30}, timestamp=3)

    rows = {r["g"]: r["avg"] for r in e.query("v")}
    assert rows["x"] == pytest.approx(20.0)


def test_avg_retraction():
    e, src = make_engine()
    view = src.group_by(["g"], {"avg": agg.Avg("v")})
    e.register_view("v", view)

    e.ingest("events",  {"g": "x", "v": 0},  timestamp=1)
    e.ingest("events",  {"g": "x", "v": 100},timestamp=2)
    e.retract("events", {"g": "x", "v": 0},  timestamp=3)

    rows = {r["g"]: r["avg"] for r in e.query("v")}
    assert rows["x"] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# MIN / MAX with retractions
# ---------------------------------------------------------------------------

def test_min_retraction():
    e, src = make_engine()
    view = src.group_by(["g"], {"mn": agg.Min("v")})
    e.register_view("v", view)

    e.ingest("events",  {"g": "x", "v": 5},  timestamp=1)
    e.ingest("events",  {"g": "x", "v": 3},  timestamp=2)
    e.ingest("events",  {"g": "x", "v": 7},  timestamp=3)

    rows = {r["g"]: r["mn"] for r in e.query("v")}
    assert rows["x"] == 3

    # Retract the minimum — new minimum should be 5
    e.retract("events", {"g": "x", "v": 3}, timestamp=4)
    rows = {r["g"]: r["mn"] for r in e.query("v")}
    assert rows["x"] == 5


def test_max_retraction():
    e, src = make_engine()
    view = src.group_by(["g"], {"mx": agg.Max("v")})
    e.register_view("v", view)

    e.ingest("events",  {"g": "x", "v": 10}, timestamp=1)
    e.ingest("events",  {"g": "x", "v": 99}, timestamp=2)
    e.retract("events", {"g": "x", "v": 99}, timestamp=3)

    rows = {r["g"]: r["mx"] for r in e.query("v")}
    assert rows["x"] == 10


# ---------------------------------------------------------------------------
# Multi-key GROUP BY
# ---------------------------------------------------------------------------

def test_multi_key_group_by():
    e, src = make_engine()
    view = src.group_by(["region", "product"], {"sales": agg.Sum("amount")})
    e.register_view("v", view)

    records = [
        {"region": "US", "product": "A", "amount": 100},
        {"region": "US", "product": "B", "amount": 200},
        {"region": "EU", "product": "A", "amount": 150},
        {"region": "US", "product": "A", "amount": 50},
    ]
    for r in records:
        e.ingest("events", r, timestamp=1)

    rows = {(r["region"], r["product"]): r["sales"] for r in e.query("v")}
    assert rows[("US", "A")] == 150
    assert rows[("US", "B")] == 200
    assert rows[("EU", "A")] == 150


# ---------------------------------------------------------------------------
# Delta log correctness
# ---------------------------------------------------------------------------

def test_delta_log_has_retract_assert_pairs():
    e, src = make_engine()
    view = src.group_by(["g"], {"n": agg.Count()})
    e.register_view("v", view)

    e.ingest("events", {"g": "x"}, timestamp=1)  # assert(n=1)
    e.ingest("events", {"g": "x"}, timestamp=2)  # retract(n=1), assert(n=2)

    log = e.delta_log("v")
    # 3 deltas: +1 (n=1), -1 (n=1), +1 (n=2)
    assert len(log) == 3
    diffs = [d.diff for d in log]
    assert diffs == [1, -1, 1]


def test_filter_then_group_by():
    e, src = make_engine()
    view = (
        src
        .filter(lambda r: r["active"])
        .group_by(["cat"], {"n": agg.Count()})
    )
    e.register_view("v", view)

    e.ingest("events", {"cat": "A", "active": True},  timestamp=1)
    e.ingest("events", {"cat": "A", "active": False}, timestamp=2)
    e.ingest("events", {"cat": "A", "active": True},  timestamp=3)

    rows = {r["cat"]: r["n"] for r in e.query("v")}
    assert rows["A"] == 2  # only active=True records
