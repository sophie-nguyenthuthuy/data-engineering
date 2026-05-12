"""
Generate synthetic TPC-H–style data (lineitem + orders) as Apache Arrow tables.

Scale factor controls row count:
  SF 0.1 → ~600K lineitem rows, ~150K orders rows
  SF 1.0 → ~6M   lineitem rows, ~1.5M  orders rows
"""
from __future__ import annotations

import random
from typing import Optional

import numpy as np
import pyarrow as pa

LINEITEM_ROWS_PER_SF = 600_000
ORDERS_ROWS_PER_SF = 150_000


def generate_lineitem(scale_factor: float = 0.1, seed: int = 42) -> pa.Table:
    rng = np.random.default_rng(seed)
    n = int(LINEITEM_ROWS_PER_SF * scale_factor)

    ship_dates = _random_dates(rng, n, "1992-01-01", "1999-01-01")
    commit_dates = _random_dates(rng, n, "1992-01-01", "1999-01-01")
    receipt_dates = _random_dates(rng, n, "1992-01-01", "1999-01-01")

    return pa.table({
        "l_orderkey":      pa.array(rng.integers(1, ORDERS_ROWS_PER_SF * scale_factor + 1, n), type=pa.int64()),
        "l_partkey":       pa.array(rng.integers(1, 200_000, n), type=pa.int64()),
        "l_suppkey":       pa.array(rng.integers(1, 10_000, n), type=pa.int64()),
        "l_linenumber":    pa.array(rng.integers(1, 7, n), type=pa.int32()),
        "l_quantity":      pa.array(rng.uniform(1, 50, n).round(2), type=pa.float64()),
        "l_extendedprice": pa.array((rng.uniform(900, 104950, n)).round(2), type=pa.float64()),
        "l_discount":      pa.array(rng.uniform(0, 0.10, n).round(2), type=pa.float64()),
        "l_tax":           pa.array(rng.uniform(0, 0.08, n).round(2), type=pa.float64()),
        "l_returnflag":    pa.array(rng.choice(["N", "A", "R"], n).tolist(), type=pa.string()),
        "l_linestatus":    pa.array(rng.choice(["O", "F"], n).tolist(), type=pa.string()),
        "l_shipdate":      pa.array(ship_dates, type=pa.string()),
        "l_commitdate":    pa.array(commit_dates, type=pa.string()),
        "l_receiptdate":   pa.array(receipt_dates, type=pa.string()),
        "l_shipinstruct":  pa.array(rng.choice(["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"], n).tolist(), type=pa.string()),
        "l_shipmode":      pa.array(rng.choice(["AIR", "REG AIR", "SHIP", "RAIL", "TRUCK", "MAIL", "FOB"], n).tolist(), type=pa.string()),
    })


def generate_orders(scale_factor: float = 0.1, seed: int = 42) -> pa.Table:
    rng = np.random.default_rng(seed + 1)
    n = int(ORDERS_ROWS_PER_SF * scale_factor)

    order_dates = _random_dates(rng, n, "1992-01-01", "1999-01-01")

    return pa.table({
        "o_orderkey":      pa.array(np.arange(1, n + 1), type=pa.int64()),
        "o_custkey":       pa.array(rng.integers(1, 150_000, n), type=pa.int64()),
        "o_orderstatus":   pa.array(rng.choice(["F", "O", "P"], n).tolist(), type=pa.string()),
        "o_totalprice":    pa.array(rng.uniform(900, 500_000, n).round(2), type=pa.float64()),
        "o_orderdate":     pa.array(order_dates, type=pa.string()),
        "o_orderpriority": pa.array(rng.choice(["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"], n).tolist(), type=pa.string()),
        "o_clerk":         pa.array([f"Clerk#{i % 1000:09d}" for i in range(n)], type=pa.string()),
        "o_shippriority":  pa.array(rng.integers(0, 1, n), type=pa.int32()),
        "o_comment":       pa.array(["comment"] * n, type=pa.string()),
    })


def _random_dates(rng: np.random.Generator, n: int, start: str, end: str) -> list[str]:
    """Generate n random date strings between start and end (YYYY-MM-DD)."""
    from datetime import date, timedelta
    start_dt = date.fromisoformat(start)
    end_dt = date.fromisoformat(end)
    delta_days = (end_dt - start_dt).days
    offsets = rng.integers(0, delta_days, n)
    return [(start_dt + timedelta(days=int(d))).isoformat() for d in offsets]
