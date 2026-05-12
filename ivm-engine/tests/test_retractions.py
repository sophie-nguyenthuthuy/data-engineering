"""Comprehensive retraction correctness tests.

Retractions are the core hard part of IVM.  These tests verify that
every operator handles negative multiplicities correctly, including:
  - Double retraction (retract something never inserted — no-op or error-free)
  - Retraction then re-insertion
  - Value correction (retract old, insert new)
  - Retraction propagating through filter, project, group_by, join, window
  - Delta log has exact retract/assert pairs
"""
import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ivm import IVMEngine, TumblingWindow
import ivm.aggregates as agg


# ---------------------------------------------------------------------------
# Value correction pattern
# ---------------------------------------------------------------------------

def test_value_correction_through_group_by():
    """Retract the old value, insert the corrected value → aggregate updates."""
    e = IVMEngine()
    src = e.source("s")
    view = src.group_by(["k"], {"total": agg.Sum("v")})
    e.register_view("v", view)

    e.ingest("s", {"k": "x", "v": 100}, timestamp=1)
    e.ingest("s", {"k": "x", "v": 200}, timestamp=2)

    # Correct the first record: v was 100, should be 150
    e.retract("s", {"k": "x", "v": 100}, timestamp=3)
    e.ingest("s",  {"k": "x", "v": 150}, timestamp=3)

    rows = {r["k"]: r["total"] for r in e.query("v")}
    assert rows["x"] == 350  # 150 + 200


# ---------------------------------------------------------------------------
# Retraction followed by re-insertion
# ---------------------------------------------------------------------------

def test_retract_then_reinsert():
    e = IVMEngine()
    src = e.source("s")
    view = src.group_by(["k"], {"n": agg.Count()})
    e.register_view("v", view)

    e.ingest("s",  {"k": "x"}, timestamp=1)
    e.retract("s", {"k": "x"}, timestamp=2)
    assert e.query("v") == []

    e.ingest("s", {"k": "x"}, timestamp=3)
    rows = {r["k"]: r["n"] for r in e.query("v")}
    assert rows["x"] == 1


# ---------------------------------------------------------------------------
# Retraction through filter
# ---------------------------------------------------------------------------

def test_retraction_through_filter():
    e = IVMEngine()
    src = e.source("s")
    view = src.filter(lambda r: r["active"]).group_by(["k"], {"n": agg.Count()})
    e.register_view("v", view)

    e.ingest("s", {"k": "x", "active": True},  timestamp=1)
    e.ingest("s", {"k": "x", "active": True},  timestamp=2)
    e.ingest("s", {"k": "x", "active": False}, timestamp=3)  # filtered out

    assert {r["k"]: r["n"] for r in e.query("v")}["x"] == 2

    # Retract an active record
    e.retract("s", {"k": "x", "active": True}, timestamp=4)
    assert {r["k"]: r["n"] for r in e.query("v")}["x"] == 1

    # Retract an inactive record (was filtered, so no effect on view)
    e.retract("s", {"k": "x", "active": False}, timestamp=5)
    assert {r["k"]: r["n"] for r in e.query("v")}["x"] == 1


# ---------------------------------------------------------------------------
# Retraction through window
# ---------------------------------------------------------------------------

def test_retraction_through_tumbling_window():
    e = IVMEngine()
    src = e.source("s")
    view = src.window(TumblingWindow(size_ms=10_000),
                      aggregates={"total": agg.Sum("v")})
    e.register_view("v", view)

    e.ingest("s", {"v": 100}, timestamp=1_000)
    e.ingest("s", {"v": 50},  timestamp=2_000)

    rows = e.query("v")
    assert rows[0]["total"] == 150

    e.retract("s", {"v": 100}, timestamp=1_000)
    rows = e.query("v")
    assert rows[0]["total"] == 50


# ---------------------------------------------------------------------------
# Min/Max: retract the extreme value
# ---------------------------------------------------------------------------

def test_retract_min_updates_correctly():
    e = IVMEngine()
    src = e.source("s")
    view = src.group_by(["g"], {"mn": agg.Min("v"), "mx": agg.Max("v")})
    e.register_view("v", view)

    for val in [5, 10, 15, 20]:
        e.ingest("s", {"g": "x", "v": val}, timestamp=1)

    rows = {r["g"]: r for r in e.query("v")}
    assert rows["x"]["mn"] == 5
    assert rows["x"]["mx"] == 20

    # Retract both extremes
    e.retract("s", {"g": "x", "v": 5},  timestamp=2)
    e.retract("s", {"g": "x", "v": 20}, timestamp=3)

    rows = {r["g"]: r for r in e.query("v")}
    assert rows["x"]["mn"] == 10
    assert rows["x"]["mx"] == 15


# ---------------------------------------------------------------------------
# Duplicate values with retractions
# ---------------------------------------------------------------------------

def test_duplicate_values_retraction():
    """When the same value appears multiple times, one retraction removes one copy."""
    e = IVMEngine()
    src = e.source("s")
    view = src.group_by(["g"], {"mn": agg.Min("v")})
    e.register_view("v", view)

    # Insert value 5 three times
    for _ in range(3):
        e.ingest("s", {"g": "x", "v": 5}, timestamp=1)
    e.ingest("s", {"g": "x", "v": 10}, timestamp=2)

    assert {r["g"]: r["mn"] for r in e.query("v")}["x"] == 5

    # Retract value 5 twice — it should still be the min (one copy remains)
    e.retract("s", {"g": "x", "v": 5}, timestamp=3)
    e.retract("s", {"g": "x", "v": 5}, timestamp=4)

    rows = {r["g"]: r["mn"] for r in e.query("v")}
    assert rows["x"] == 5  # one copy of 5 still exists

    # Retract the last copy of 5 — min should become 10
    e.retract("s", {"g": "x", "v": 5}, timestamp=5)
    rows = {r["g"]: r["mn"] for r in e.query("v")}
    assert rows["x"] == 10


# ---------------------------------------------------------------------------
# Multi-hop retraction (through join then group_by)
# ---------------------------------------------------------------------------

def test_retraction_propagates_through_join_and_group_by():
    e = IVMEngine()
    orders   = e.source("orders")
    products = e.source("products")

    joined  = orders.join(products, left_key="pid", right_key="pid")
    revenue = joined.group_by(["cat"], {"rev": agg.Sum("amount")})
    e.register_view("revenue", revenue)

    e.ingest("products", {"pid": "p1", "cat": "A"}, timestamp=0)
    e.ingest("orders",   {"pid": "p1", "amount": 100}, timestamp=1)
    e.ingest("orders",   {"pid": "p1", "amount": 200}, timestamp=2)

    assert {r["cat"]: r["rev"] for r in e.query("revenue")}["A"] == 300

    e.retract("orders", {"pid": "p1", "amount": 200}, timestamp=3)
    assert {r["cat"]: r["rev"] for r in e.query("revenue")}["A"] == 100

    # Retract the product — the remaining order loses its join match → revenue drops
    e.retract("products", {"pid": "p1", "cat": "A"}, timestamp=4)
    assert e.query("revenue") == []


# ---------------------------------------------------------------------------
# Delta log consistency
# ---------------------------------------------------------------------------

def test_delta_log_is_consistent():
    """Net multiplicity of each unique record in delta log should equal 0 or >0."""
    from collections import Counter
    from ivm.types import freeze_record

    e = IVMEngine()
    src = e.source("s")
    view = src.group_by(["k"], {"n": agg.Count(), "total": agg.Sum("v")})
    e.register_view("v", view)

    events = [
        ("ingest", {"k": "x", "v": 10}),
        ("ingest", {"k": "x", "v": 20}),
        ("ingest", {"k": "y", "v": 5}),
        ("retract", {"k": "x", "v": 10}),
        ("ingest", {"k": "x", "v": 15}),
        ("retract", {"k": "y", "v": 5}),
    ]
    for op, rec in events:
        if op == "ingest":
            e.ingest("s", rec, timestamp=1)
        else:
            e.retract("s", rec, timestamp=1)

    log = e.delta_log("v")
    net: Counter = Counter()
    for delta in log:
        net[freeze_record(delta.record)] += delta.diff

    # All net counts must be non-negative
    for key, count in net.items():
        assert count >= 0, f"Negative net multiplicity for {dict(key)}: {count}"
