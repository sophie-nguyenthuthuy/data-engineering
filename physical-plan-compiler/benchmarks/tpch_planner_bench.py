"""Benchmark: compile representative TPC-H queries across scale factors.

For each (query, scale_factor) pair we report:
  - chosen engine
  - estimated total cost
  - planner wall time (median of N runs)
"""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass

from ppc.cascades.optimizer import Optimizer
from ppc.frontend.catalog import Catalog
from ppc.frontend.sql import sql_to_logical
from ppc.ir.schema import Column, Schema, Stats
from ppc.ir.types import DOUBLE, INT64, STRING, TIMESTAMP


def tpch_catalog(scale: int) -> Catalog:
    cat = Catalog()
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
        Column("c_mktsegment", STRING, Stats(ndv=5)),
        rows=150_000 * scale,
    ))
    cat.register("orders", Schema.of(
        Column("o_orderkey", INT64, Stats(ndv=1_500_000 * scale)),
        Column("o_custkey", INT64, Stats(ndv=150_000 * scale)),
        Column("o_orderdate", TIMESTAMP),
        Column("o_totalprice", DOUBLE),
        Column("o_orderstatus", STRING, Stats(ndv=3)),
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


QUERIES = {
    "Q1": (
        "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, "
        "       SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order "
        "FROM lineitem WHERE l_shipdate <= '1998-12-01' "
        "GROUP BY l_returnflag, l_linestatus"
    ),
    "Q3": (
        "SELECT l_orderkey, SUM(l_extendedprice) AS revenue FROM customer c "
        "JOIN orders o ON c.c_custkey = o.o_custkey "
        "JOIN lineitem l ON l.l_orderkey = o.o_orderkey "
        "WHERE c.c_mktsegment = 'BUILDING' AND o.o_orderdate < '1995-03-15' "
        "AND l.l_shipdate > '1995-03-15' "
        "GROUP BY l_orderkey"
    ),
    "Q6": (
        "SELECT SUM(l_extendedprice) AS revenue FROM lineitem "
        "WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' "
        "AND l_discount > 0.05 AND l_discount < 0.07 AND l_quantity < 24"
    ),
}


@dataclass
class BenchResult:
    query: str
    scale: int
    engine: str
    cost: float
    rows_estimate: int | None
    median_plan_us: float


def bench_one(query_name: str, sql: str, scale: int, trials: int = 20) -> BenchResult:
    cat = tpch_catalog(scale)
    optimizer = Optimizer(catalog=cat)
    times: list[float] = []
    plan = None
    for _ in range(trials):
        t0 = time.perf_counter_ns()
        logical = sql_to_logical(sql, cat)
        plan = optimizer.optimize(logical)
        times.append((time.perf_counter_ns() - t0) / 1000.0)  # µs
    assert plan is not None
    return BenchResult(
        query=query_name, scale=scale, engine=plan.root.engine,
        cost=plan.total_cost, rows_estimate=plan.logical.schema.rows,
        median_plan_us=statistics.median(times),
    )


def main() -> None:
    print(f"{'Query':<6} {'Scale':>6} {'Engine':<8} {'Cost':>14} "
          f"{'Rows':>10}  {'Plan (µs)':>10}")
    print("-" * 70)
    for q_name, sql in QUERIES.items():
        for sf in (1, 10, 100, 1000):
            r = bench_one(q_name, sql, sf)
            print(f"{r.query:<6} {r.scale:>6} {r.engine:<8} {r.cost:>14,.2f} "
                  f"{r.rows_estimate or 0:>10,}  {r.median_plan_us:>10.1f}")


if __name__ == "__main__":
    main()
