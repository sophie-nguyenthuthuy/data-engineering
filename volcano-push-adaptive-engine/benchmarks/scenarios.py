"""Pre-built benchmark scenarios.

Each scenario returns a list of (catalog, plan) pairs parameterised by
a sweep variable (table size, selectivity, join fanout, group count).
"""
from __future__ import annotations
import random

from adaptive_engine import (
    AggregateNode,
    Catalog,
    FilterNode,
    HashJoinNode,
    ProjectNode,
    ScanNode,
    SortNode,
    gt,
    eq,
)
from adaptive_engine.expressions import BinOp, ColRef, Literal

from .runner import Scenario

_rng = random.Random(42)


# ------------------------------------------------------------------
# 1. Filter selectivity sweep
# ------------------------------------------------------------------

def _filter_selectivity_factory(selectivity: float) -> tuple[Catalog, object]:
    n = 100_000
    data = [{"id": i, "value": i} for i in range(n)]
    catalog = Catalog()
    catalog.create_table("t", data)
    threshold = int(n * (1.0 - selectivity))
    plan = FilterNode(
        child=ScanNode(table="t"),
        predicate=gt("value", threshold),
        selectivity=selectivity,
    )
    return catalog, plan


FILTER_SELECTIVITY = Scenario(
    name="Filter Selectivity Sweep",
    param_name="selectivity",
    param_values=[0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0],
    factory=_filter_selectivity_factory,
    description="100k-row table; vary what fraction of rows pass the filter",
)


# ------------------------------------------------------------------
# 2. Table size sweep
# ------------------------------------------------------------------

def _table_size_factory(n: int) -> tuple[Catalog, object]:
    data = [{"id": i, "value": _rng.randint(0, n)} for i in range(n)]
    catalog = Catalog()
    catalog.create_table("t", data)
    threshold = n // 2
    plan = FilterNode(
        child=ScanNode(table="t"),
        predicate=gt("value", threshold),
        selectivity=0.5,
    )
    return catalog, plan


TABLE_SIZE = Scenario(
    name="Table Size Sweep",
    param_name="n_rows",
    param_values=[1_000, 5_000, 10_000, 50_000, 100_000, 500_000],
    factory=_table_size_factory,
    description="50% selectivity filter; vary table size",
)


# ------------------------------------------------------------------
# 3. Hash join — probe-side size sweep
# ------------------------------------------------------------------

def _join_size_factory(probe_n: int) -> tuple[Catalog, object]:
    build_n = 1_000
    probe = [{"id": i, "key": _rng.randint(0, build_n - 1), "val": i} for i in range(probe_n)]
    build = [{"key": i, "label": f"L{i}"} for i in range(build_n)]

    catalog = Catalog()
    catalog.create_table("probe", probe)
    catalog.create_table("build", build)

    plan = HashJoinNode(
        left=ScanNode(table="probe"),
        right=ScanNode(table="build"),
        left_key="key",
        right_key="key",
    )
    return catalog, plan


JOIN_PROBE_SIZE = Scenario(
    name="Join Probe-Side Size Sweep",
    param_name="probe_rows",
    param_values=[500, 1_000, 5_000, 10_000, 50_000, 100_000],
    factory=_join_size_factory,
    description="1k-row build side; vary probe-side size",
)


# ------------------------------------------------------------------
# 4. Aggregate group count sweep
# ------------------------------------------------------------------

def _agg_groups_factory(n_groups: int) -> tuple[Catalog, object]:
    n = 200_000
    data = [{"id": i, "group": _rng.randint(0, n_groups - 1), "value": _rng.random()} for i in range(n)]
    catalog = Catalog()
    catalog.create_table("t", data)

    plan = AggregateNode(
        child=ScanNode(table="t"),
        group_by=["group"],
        aggregates=[
            ("cnt", "count", "id"),
            ("total", "sum", "value"),
            ("avg_val", "avg", "value"),
        ],
    )
    return catalog, plan


AGGREGATE_GROUPS = Scenario(
    name="Aggregate Group Count Sweep",
    param_name="n_groups",
    param_values=[1, 10, 100, 1_000, 10_000, 50_000],
    factory=_agg_groups_factory,
    description="200k rows; vary number of distinct group-by values",
)


# ------------------------------------------------------------------
# 5. End-to-end pipeline (filter + join + agg + sort)
# ------------------------------------------------------------------

def _pipeline_factory(n: int) -> tuple[Catalog, object]:
    orders = [
        {
            "order_id": i,
            "product_id": _rng.randint(0, 999),
            "amount": _rng.uniform(1, 500),
            "status": _rng.choice(["open", "closed", "pending"]),
        }
        for i in range(n)
    ]
    products = [{"product_id": i, "category": f"cat{i % 10}"} for i in range(1000)]

    catalog = Catalog()
    catalog.create_table("orders", orders)
    catalog.create_table("products", products)

    plan = SortNode(
        child=AggregateNode(
            child=HashJoinNode(
                left=FilterNode(
                    child=ScanNode(table="orders"),
                    predicate=eq("status", "open"),
                    selectivity=0.33,
                ),
                right=ScanNode(table="products"),
                left_key="product_id",
                right_key="product_id",
            ),
            group_by=["category"],
            aggregates=[("total", "sum", "amount"), ("cnt", "count", "order_id")],
        ),
        order_by=[("total", False)],
    )
    return catalog, plan


PIPELINE = Scenario(
    name="Full Pipeline (filter+join+agg+sort)",
    param_name="order_rows",
    param_values=[1_000, 5_000, 10_000, 50_000, 100_000],
    factory=_pipeline_factory,
    description="Filter open orders → join products → group by category → sort",
)


ALL_SCENARIOS = [
    FILTER_SELECTIVITY,
    TABLE_SIZE,
    JOIN_PROBE_SIZE,
    AGGREGATE_GROUPS,
    PIPELINE,
]
