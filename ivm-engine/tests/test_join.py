"""Tests for JoinOperator — inner join, left join, and retraction propagation."""
import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ivm import IVMEngine
import ivm.aggregates as agg


# ---------------------------------------------------------------------------
# INNER JOIN basics
# ---------------------------------------------------------------------------

def test_inner_join_left_arrives_first():
    e = IVMEngine()
    left  = e.source("left")
    right = e.source("right")
    joined = left.join(right, left_key="id", right_key="id")
    e.register_view("j", joined)

    e.ingest("left",  {"id": 1, "lval": "A"}, timestamp=1)
    e.ingest("right", {"id": 1, "rval": "B"}, timestamp=2)

    rows = e.query("j")
    assert len(rows) == 1
    assert rows[0]["lval"] == "A"
    assert rows[0]["rval"] == "B"


def test_inner_join_right_arrives_first():
    e = IVMEngine()
    left  = e.source("left")
    right = e.source("right")
    joined = left.join(right, left_key="id", right_key="id")
    e.register_view("j", joined)

    e.ingest("right", {"id": 1, "rval": "B"}, timestamp=1)
    e.ingest("left",  {"id": 1, "lval": "A"}, timestamp=2)

    rows = e.query("j")
    assert len(rows) == 1
    assert rows[0]["lval"] == "A"
    assert rows[0]["rval"] == "B"


def test_inner_join_no_match():
    e = IVMEngine()
    left  = e.source("left")
    right = e.source("right")
    joined = left.join(right, left_key="id", right_key="id")
    e.register_view("j", joined)

    e.ingest("left",  {"id": 1, "lval": "A"}, timestamp=1)
    e.ingest("right", {"id": 2, "rval": "B"}, timestamp=2)

    assert e.query("j") == []


def test_inner_join_one_to_many():
    e = IVMEngine()
    left  = e.source("left")
    right = e.source("right")
    joined = left.join(right, left_key="cat", right_key="cat")
    e.register_view("j", joined)

    e.ingest("left", {"cat": "X", "lval": "L"}, timestamp=1)
    e.ingest("right", {"cat": "X", "rval": "R1"}, timestamp=2)
    e.ingest("right", {"cat": "X", "rval": "R2"}, timestamp=3)

    rows = e.query("j")
    assert len(rows) == 2
    rvals = {r["rval"] for r in rows}
    assert rvals == {"R1", "R2"}


def test_inner_join_many_to_many():
    e = IVMEngine()
    left  = e.source("left")
    right = e.source("right")
    joined = left.join(right, left_key="k", right_key="k")
    e.register_view("j", joined)

    for i in range(3):
        e.ingest("left",  {"k": "x", "l": i}, timestamp=1)
    for j in range(2):
        e.ingest("right", {"k": "x", "r": j}, timestamp=2)

    assert len(e.query("j")) == 6  # 3 × 2


# ---------------------------------------------------------------------------
# Retraction through join
# ---------------------------------------------------------------------------

def test_left_retraction_removes_join_output():
    e = IVMEngine()
    left  = e.source("left")
    right = e.source("right")
    joined = left.join(right, left_key="id", right_key="id")
    e.register_view("j", joined)

    e.ingest("left",  {"id": 1, "lval": "A"}, timestamp=1)
    e.ingest("right", {"id": 1, "rval": "B"}, timestamp=2)
    assert len(e.query("j")) == 1

    e.retract("left", {"id": 1, "lval": "A"}, timestamp=3)
    assert e.query("j") == []


def test_right_retraction_removes_join_output():
    e = IVMEngine()
    left  = e.source("left")
    right = e.source("right")
    joined = left.join(right, left_key="id", right_key="id")
    e.register_view("j", joined)

    e.ingest("left",  {"id": 1, "lval": "A"}, timestamp=1)
    e.ingest("right", {"id": 1, "rval": "B"}, timestamp=2)
    e.retract("right", {"id": 1, "rval": "B"}, timestamp=3)

    assert e.query("j") == []


def test_right_retraction_partial():
    e = IVMEngine()
    left  = e.source("left")
    right = e.source("right")
    joined = left.join(right, left_key="k", right_key="k")
    e.register_view("j", joined)

    e.ingest("left",  {"k": "x", "l": 1}, timestamp=1)
    e.ingest("right", {"k": "x", "r": "a"}, timestamp=2)
    e.ingest("right", {"k": "x", "r": "b"}, timestamp=3)

    assert len(e.query("j")) == 2

    e.retract("right", {"k": "x", "r": "a"}, timestamp=4)
    rows = e.query("j")
    assert len(rows) == 1
    assert rows[0]["r"] == "b"


# ---------------------------------------------------------------------------
# Join into GROUP BY (multi-hop)
# ---------------------------------------------------------------------------

def test_join_then_group_by():
    e = IVMEngine()
    orders   = e.source("orders")
    products = e.source("products")

    joined = orders.join(products, left_key="pid", right_key="pid")
    revenue = joined.group_by(["cat"], {"rev": agg.Sum("amount")})
    e.register_view("revenue", revenue)

    for p in [{"pid": "p1", "cat": "A"}, {"pid": "p2", "cat": "B"}]:
        e.ingest("products", p, timestamp=0)

    for o in [
        {"pid": "p1", "amount": 100},
        {"pid": "p1", "amount": 200},
        {"pid": "p2", "amount": 50},
    ]:
        e.ingest("orders", o, timestamp=1)

    rows = {r["cat"]: r["rev"] for r in e.query("revenue")}
    assert rows["A"] == 300
    assert rows["B"] == 50

    # Retract one order — aggregate should update
    e.retract("orders", {"pid": "p1", "amount": 100}, timestamp=2)
    rows = {r["cat"]: r["rev"] for r in e.query("revenue")}
    assert rows["A"] == 200


# ---------------------------------------------------------------------------
# LEFT JOIN
# ---------------------------------------------------------------------------

def test_left_join_unmatched_rows():
    e = IVMEngine()
    left  = e.source("left")
    right = e.source("right")
    joined = left.join(right, left_key="id", right_key="id", join_type="left")
    e.register_view("j", joined)

    e.ingest("left", {"id": 1, "lval": "A"}, timestamp=1)
    e.ingest("left", {"id": 2, "lval": "B"}, timestamp=2)

    rows = e.query("j")
    assert len(rows) == 2

    # Only id=1 gets a match
    e.ingest("right", {"id": 1, "rval": "X"}, timestamp=3)
    rows = e.query("j")
    assert len(rows) == 2

    matched   = next(r for r in rows if r["id"] == 1)
    unmatched = next(r for r in rows if r["id"] == 2)
    assert matched.get("rval") == "X"
    assert "rval" not in unmatched  # NULL for left-join unmatched


def test_left_join_retract_right_restores_unmatched():
    e = IVMEngine()
    left  = e.source("left")
    right = e.source("right")
    joined = left.join(right, left_key="id", right_key="id", join_type="left")
    e.register_view("j", joined)

    e.ingest("left",  {"id": 1, "lval": "A"}, timestamp=1)
    e.ingest("right", {"id": 1, "rval": "X"}, timestamp=2)

    rows = e.query("j")
    assert len(rows) == 1
    assert rows[0].get("rval") == "X"

    # Retract right match — left row should still appear (as unmatched)
    e.retract("right", {"id": 1, "rval": "X"}, timestamp=3)
    rows = e.query("j")
    assert len(rows) == 1
    assert "rval" not in rows[0]
