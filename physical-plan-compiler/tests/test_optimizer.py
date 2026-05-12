"""Optimizer / Cascades core."""

from __future__ import annotations

from ppc.cascades.optimizer import Optimizer
from ppc.engines.physical_ops import (
    PhysicalFilter,
    PhysicalHashJoin,
    PhysicalScan,
)
from ppc.frontend.sql import sql_to_logical


def _plan(sql, catalog):
    logical = sql_to_logical(sql, catalog)
    return Optimizer(catalog=catalog).optimize(logical)


def test_simple_scan_yields_physical_scan(small_catalog):
    p = _plan("SELECT * FROM orders", small_catalog)
    assert isinstance(p.root, PhysicalScan)
    assert p.root.engine in ("spark", "dbt", "duckdb", "flink")


def test_small_data_prefers_duckdb(small_catalog):
    p = _plan("SELECT * FROM orders WHERE o_totalprice > 100", small_catalog)
    # SF=1 → ~90 MB; DuckDB has tiny setup → wins
    assert p.root.engine == "duckdb"


def test_huge_data_avoids_duckdb(huge_catalog):
    """SF=1000 → ~90 GB lineitem; exceeds DuckDB's 8 GB cap."""
    p = _plan(
        "SELECT l_returnflag, SUM(l_extendedprice) FROM lineitem "
        "WHERE l_quantity > 25 GROUP BY l_returnflag",
        huge_catalog,
    )
    # Should NOT pick duckdb (spill penalty would dominate)
    assert p.root.engine != "duckdb"


def test_filter_pushdown_through_join(small_catalog):
    """Filter on a single side of a join should be pushed below it."""
    p = _plan(
        "SELECT c_name, o_totalprice FROM customer c "
        "JOIN orders o ON c.c_custkey = o.o_custkey "
        "WHERE o_totalprice > 100",
        small_catalog,
    )
    # After pushdown the join should see a smaller orders input.
    # The plan tree should contain a Filter under a HashJoin (rather than
    # Filter above HashJoin).
    def walk(node, parent):
        yield node, parent
        for c in node.children:
            yield from walk(c, node)

    filters_under_join = 0
    filters_above_join = 0
    for node, parent in walk(p.root, None):
        if isinstance(node, PhysicalFilter):
            if isinstance(parent, PhysicalHashJoin):
                filters_under_join += 1
            else:
                # Filter is the root or under a non-join op
                filters_above_join += 1
    # At least one filter should now be under the join
    assert filters_under_join >= 1, p.explain()


def test_no_op_filter_on_unfilterable_pred(small_catalog):
    """Predicate referencing both sides of join CANNOT be pushed down."""
    p = _plan(
        "SELECT * FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey "
        "WHERE c.c_acctbal > o.o_totalprice",
        small_catalog,
    )
    # Filter cannot push past join → must sit on top
    assert isinstance(p.root, PhysicalFilter)


def test_plan_includes_all_referenced_tables(small_catalog):
    p = _plan(
        "SELECT n_name, SUM(o_totalprice) FROM orders o "
        "JOIN customer c ON o.o_custkey = c.c_custkey "
        "JOIN nation n ON c.c_nationkey = n.n_nationkey "
        "GROUP BY n_name",
        small_catalog,
    )
    found = set()

    def walk(node):
        if isinstance(node, PhysicalScan):
            found.add(node.table)
        for c in node.children:
            walk(c)

    walk(p.root)
    assert found == {"orders", "customer", "nation"}


def test_cost_is_positive(small_catalog):
    p = _plan("SELECT * FROM orders WHERE o_totalprice > 100", small_catalog)
    assert p.total_cost > 0


def test_deterministic(small_catalog):
    p1 = _plan("SELECT * FROM orders WHERE o_totalprice > 100", small_catalog)
    p2 = _plan("SELECT * FROM orders WHERE o_totalprice > 100", small_catalog)
    assert p1.total_cost == p2.total_cost
    assert p1.root.engine == p2.root.engine


def test_engine_consistency_within_one_plan(small_catalog):
    """Without conversion ops, every op should share the same engine."""
    from ppc.engines.physical_ops import PhysicalConversion

    p = _plan(
        "SELECT o_orderstatus, COUNT(*) FROM orders GROUP BY o_orderstatus",
        small_catalog,
    )
    engines = set()

    def walk(node):
        if not isinstance(node, PhysicalConversion):
            engines.add(node.engine)
        for c in node.children:
            walk(c)

    walk(p.root)
    # All on same engine (since the optimizer should not insert a conversion
    # when it can stay on one engine).
    assert len(engines) == 1, f"unexpected engine mix: {engines}"
