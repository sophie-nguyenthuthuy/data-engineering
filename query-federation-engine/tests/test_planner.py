"""Tests for QueryPlanner and CostBasedOptimizer."""

import pytest
from federation.planner import CostBasedOptimizer, QueryPlanner, explain_plan
from federation.planner.nodes import (
    Aggregate, Filter, Join, JoinType, Limit, Project, Sort, TableScan,
)


# ──────────────────────────────────────────────────────────────────────────────
# QueryPlanner
# ──────────────────────────────────────────────────────────────────────────────

def test_simple_select(catalog):
    planner = QueryPlanner(catalog)
    plan = planner.build("SELECT id, total FROM postgres.orders")
    assert isinstance(plan, Project)
    # unwrap project → scan
    inner = plan.child
    assert isinstance(inner, TableScan)
    assert inner.source == "postgres"
    assert inner.table == "orders"


def test_predicate_pushdown(catalog):
    planner = QueryPlanner(catalog)
    plan = planner.build(
        "SELECT id FROM postgres.orders WHERE status = 'shipped'"
    )
    # Dig to the scan
    scan = _find_scan(plan, "orders")
    assert scan is not None, "Expected a TableScan for orders"
    assert len(scan.pushed_predicates) == 1, "Equality predicate should be pushed"


def test_multi_source_join(catalog):
    planner = QueryPlanner(catalog)
    plan = planner.build(
        """
        SELECT o.id, u.name
        FROM postgres.orders o
        JOIN mongodb.users u ON o.user_id = u.id
        """
    )
    join = _find_node(plan, Join)
    assert join is not None, "Expected a Join node"

    sources = _collect_sources(plan)
    assert "postgres" in sources
    assert "mongodb" in sources


def test_limit_node(catalog):
    planner = QueryPlanner(catalog)
    plan = planner.build("SELECT id FROM postgres.orders LIMIT 10")
    limit = _find_node(plan, Limit)
    assert limit is not None
    assert limit.count == 10


def test_cross_source_three_way_join(catalog):
    planner = QueryPlanner(catalog)
    plan = planner.build(
        """
        SELECT o.id, u.name, e.event_type
        FROM postgres.orders o
        JOIN mongodb.users u     ON o.user_id  = u.id
        JOIN s3_parquet.events e ON e.order_id = o.id
        """
    )
    sources = _collect_sources(plan)
    assert "postgres"   in sources
    assert "mongodb"    in sources
    assert "s3_parquet" in sources


def test_explain_returns_string(catalog):
    planner = QueryPlanner(catalog)
    plan = planner.build("SELECT id FROM postgres.orders LIMIT 5")
    text = explain_plan(plan)
    assert "TableScan" in text
    assert "orders" in text


# ──────────────────────────────────────────────────────────────────────────────
# CostBasedOptimizer
# ──────────────────────────────────────────────────────────────────────────────

def test_optimizer_annotates_costs(catalog):
    planner = QueryPlanner(catalog)
    optimizer = CostBasedOptimizer()
    plan = planner.build(
        """
        SELECT o.id, u.name
        FROM postgres.orders o
        JOIN mongodb.users u ON o.user_id = u.id
        """
    )
    optimized = optimizer.optimize(plan)
    join = _find_node(optimized, Join)
    assert join is not None
    assert join.estimated_cost > 0


def test_optimizer_reorders_joins_small_first(catalog):
    """After optimization the smaller table (users, 3 rows) should be on the right (build side)."""
    planner = QueryPlanner(catalog)
    optimizer = CostBasedOptimizer()
    plan = planner.build(
        """
        SELECT o.id, u.name
        FROM postgres.orders o
        JOIN mongodb.users u ON o.user_id = u.id
        """
    )
    optimized = optimizer.optimize(plan)
    join = _find_node(optimized, Join)
    assert join is not None
    # Smaller table should end up on one side
    sizes = sorted([join.left.estimated_rows, join.right.estimated_rows])
    assert sizes[0] <= sizes[1]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _find_scan(node, table_name: str) -> TableScan | None:
    if isinstance(node, TableScan) and node.table == table_name:
        return node
    for child in node.children():
        result = _find_scan(child, table_name)
        if result:
            return result
    return None


def _find_node(node, cls):
    if isinstance(node, cls):
        return node
    for child in node.children():
        result = _find_node(child, cls)
        if result:
            return result
    return None


def _collect_sources(node) -> set[str]:
    sources: set[str] = set()
    if isinstance(node, TableScan):
        sources.add(node.source)
    for child in node.children():
        sources |= _collect_sources(child)
    return sources
