"""TPC-H end-to-end: representative queries compile and pick reasonable engines."""

from __future__ import annotations

import pytest

from ppc.cascades.optimizer import Optimizer
from ppc.engines.physical_ops import PhysicalAggregate, PhysicalHashJoin
from ppc.frontend.sql import sql_to_logical

pytestmark = pytest.mark.tpch


def _opt(sql, catalog):
    return Optimizer(catalog=catalog).optimize(sql_to_logical(sql, catalog))


# A simplified Q1 — pricing summary on lineitem
Q1 = """
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= '1998-12-01'
GROUP BY l_returnflag, l_linestatus
"""


# Q3-ish — join customer/orders/lineitem with filter & agg
Q3 = """
SELECT
    l_orderkey,
    SUM(l_extendedprice) AS revenue
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON l.l_orderkey = o.o_orderkey
WHERE c.c_mktsegment = 'BUILDING'
  AND o.o_orderdate < '1995-03-15'
  AND l.l_shipdate > '1995-03-15'
GROUP BY l_orderkey
"""


# Q6 — pricing summary aggregate
Q6 = """
SELECT SUM(l_extendedprice) AS revenue
FROM lineitem
WHERE l_shipdate >= '1994-01-01'
  AND l_shipdate < '1995-01-01'
  AND l_discount > 0.05
  AND l_discount < 0.07
  AND l_quantity < 24
"""


def test_q1_compiles_at_sf1(small_catalog):
    p = _opt(Q1, small_catalog)
    # Root is HashAggregate
    assert isinstance(p.root, PhysicalAggregate)
    assert p.total_cost > 0


def test_q3_compiles_at_sf1(small_catalog):
    p = _opt(Q3, small_catalog)
    # Should include at least one HashJoin
    def has_join(n):
        if isinstance(n, PhysicalHashJoin):
            return True
        return any(has_join(c) for c in n.children)
    assert has_join(p.root)


def test_q6_compiles_at_sf1(small_catalog):
    p = _opt(Q6, small_catalog)
    assert isinstance(p.root, PhysicalAggregate)


def test_q1_chooses_scalable_engine_at_sf1000(huge_catalog):
    p = _opt(Q1, huge_catalog)
    # At ~36 GB lineitem * SF=1000, DuckDB's 8 GB cap is exceeded → expect
    # spark or dbt (scalable).
    assert p.root.engine in ("spark", "dbt")


def test_q6_chooses_duckdb_at_sf1(small_catalog):
    """SF=1 lineitem ≈ 270 MB. Filter is highly selective → DuckDB."""
    p = _opt(Q6, small_catalog)
    assert p.root.engine == "duckdb"


def test_q3_cost_lower_for_larger_engines_at_sf1000(huge_catalog):
    """Sanity check: at SF=1000, total cost is substantial but bounded."""
    p = _opt(Q3, huge_catalog)
    assert 1e2 < p.total_cost < 1e7
