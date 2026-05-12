"""Logical plan IR + schema derivation."""

from __future__ import annotations

import pytest

from ppc.ir.expr import column, lit
from ppc.ir.logical import (
    AggFunc,
    LogicalAggregate,
    LogicalFilter,
    LogicalJoin,
    LogicalScan,
)
from ppc.ir.schema import Column, Schema, Stats
from ppc.ir.types import DOUBLE, INT64, STRING


@pytest.fixture
def orders_schema() -> Schema:
    return Schema.of(
        Column("o_orderkey", INT64, Stats(ndv=1_000_000)),
        Column("o_custkey", INT64, Stats(ndv=100_000)),
        Column("o_status", STRING, Stats(ndv=3)),
        Column("o_total", DOUBLE),
        rows=1_000_000,
    )


def test_scan_passes_through_schema(orders_schema):
    s = LogicalScan(table="orders", table_schema=orders_schema)
    assert s.schema is orders_schema
    assert s.children == ()


def test_filter_preserves_schema(orders_schema):
    s = LogicalScan(table="orders", table_schema=orders_schema)
    f = LogicalFilter(child=s, predicate=column("o_total", DOUBLE) > lit(100))
    assert f.schema.names == orders_schema.names
    assert f.schema.rows == orders_schema.rows  # IVM-like — row count unchanged in logical


def test_aggregate_schema_derivation(orders_schema):
    s = LogicalScan(table="orders", table_schema=orders_schema)
    a = LogicalAggregate(
        child=s,
        group_by=(column("o_status", STRING),),
        aggregates=(
            AggFunc(func="COUNT", arg=None, alias="cnt"),
            AggFunc(func="SUM", arg=column("o_total", DOUBLE), alias="total"),
            AggFunc(func="AVG", arg=column("o_total", DOUBLE), alias="avg_total"),
        ),
    )
    names = a.schema.names
    assert names == ("o_status", "cnt", "total", "avg_total")
    # Group cardinality = NDV(o_status) = 3
    assert a.schema.rows == 3


def test_aggregate_count_returns_int64(orders_schema):
    s = LogicalScan(table="orders", table_schema=orders_schema)
    a = LogicalAggregate(
        child=s,
        group_by=(),
        aggregates=(AggFunc(func="COUNT", arg=None, alias="cnt"),),
    )
    assert a.schema["cnt"].dtype == INT64


def test_aggregate_avg_returns_double(orders_schema):
    s = LogicalScan(table="orders", table_schema=orders_schema)
    a = LogicalAggregate(
        child=s,
        group_by=(),
        aggregates=(AggFunc(func="AVG", arg=column("o_total", DOUBLE), alias="avg"),),
    )
    assert a.schema["avg"].dtype == DOUBLE


def test_join_row_estimate_uses_max_ndv(orders_schema):
    cust = Schema.of(
        Column("c_custkey", INT64, Stats(ndv=100_000)),
        Column("c_name", STRING),
        rows=100_000,
    )
    s_o = LogicalScan(table="orders", table_schema=orders_schema)
    s_c = LogicalScan(table="customer", table_schema=cust)
    on = column("o_custkey", INT64).eq(column("c_custkey", INT64))
    j = LogicalJoin(left=s_o, right=s_c, on=on)
    # 1M × 100K / max(100K, 100K) = 1M
    assert j.schema.rows == 1_000_000


def test_join_returns_union_columns(orders_schema):
    cust = Schema.of(
        Column("c_custkey", INT64, Stats(ndv=100_000)),
        Column("c_name", STRING),
        rows=100_000,
    )
    on = column("o_custkey", INT64).eq(column("c_custkey", INT64))
    j = LogicalJoin(left=LogicalScan(table="orders", table_schema=orders_schema),
                    right=LogicalScan(table="customer", table_schema=cust),
                    on=on)
    # All orders cols + all customer cols (dedup by name)
    assert "o_orderkey" in j.schema.names
    assert "c_name" in j.schema.names


def test_explain_recursive(orders_schema):
    s = LogicalScan(table="orders", table_schema=orders_schema)
    f = LogicalFilter(child=s, predicate=column("o_total", DOUBLE) > lit(100))
    text = f.explain()
    assert "Filter" in text
    assert "Scan" in text
    assert text.index("Filter") < text.index("Scan")  # parent comes first
