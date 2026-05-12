"""Shared fixtures: TPC-H-shaped catalog at three scale factors."""

from __future__ import annotations

import pytest

from ppc.frontend.catalog import Catalog
from ppc.ir.schema import Column, Schema, Stats
from ppc.ir.types import DOUBLE, INT64, STRING, TIMESTAMP


def _tpch_catalog(scale: int) -> Catalog:
    """Build a TPC-H-ish catalog scaled to `scale` × 1500k orders rows."""
    cat = Catalog()
    cat.register("region", Schema.of(
        Column("r_regionkey", INT64, Stats(ndv=5)),
        Column("r_name", STRING, Stats(ndv=5)),
        rows=5,
    ))
    cat.register("nation", Schema.of(
        Column("n_nationkey", INT64, Stats(ndv=25)),
        Column("n_name", STRING, Stats(ndv=25)),
        Column("n_regionkey", INT64, Stats(ndv=5)),
        rows=25,
    ))
    cat.register("customer", Schema.of(
        Column("c_custkey", INT64, Stats(ndv=150_000 * scale)),
        Column("c_name", STRING),
        Column("c_nationkey", INT64, Stats(ndv=25)),
        Column("c_acctbal", DOUBLE),
        Column("c_mktsegment", STRING, Stats(ndv=5)),
        rows=150_000 * scale,
    ))
    cat.register("orders", Schema.of(
        Column("o_orderkey", INT64, Stats(ndv=1_500_000 * scale)),
        Column("o_custkey", INT64, Stats(ndv=150_000 * scale)),
        Column("o_orderdate", TIMESTAMP),
        Column("o_totalprice", DOUBLE),
        Column("o_orderstatus", STRING, Stats(ndv=3)),
        Column("o_orderpriority", STRING, Stats(ndv=5)),
        rows=1_500_000 * scale,
    ))
    cat.register("lineitem", Schema.of(
        Column("l_orderkey", INT64, Stats(ndv=1_500_000 * scale)),
        Column("l_partkey", INT64),
        Column("l_quantity", DOUBLE),
        Column("l_extendedprice", DOUBLE),
        Column("l_discount", DOUBLE),
        Column("l_tax", DOUBLE),
        Column("l_returnflag", STRING, Stats(ndv=3)),
        Column("l_linestatus", STRING, Stats(ndv=2)),
        Column("l_shipdate", TIMESTAMP),
        rows=6_000_000 * scale,
    ))
    return cat


@pytest.fixture
def small_catalog() -> Catalog:
    """SF=1 (~10 MB data) — fits in laptop memory."""
    return _tpch_catalog(scale=1)


@pytest.fixture
def medium_catalog() -> Catalog:
    """SF=100 — borderline; some engines spill."""
    return _tpch_catalog(scale=100)


@pytest.fixture
def huge_catalog() -> Catalog:
    """SF=1000 (~1 TB) — DuckDB must give up, Spark/dbt win."""
    return _tpch_catalog(scale=1000)
