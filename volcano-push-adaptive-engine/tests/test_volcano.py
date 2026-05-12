"""Tests for volcano (pull-based) executor."""
import pytest
from adaptive_engine import (
    Catalog,
    VolcanoExecutor,
    ScanNode,
    FilterNode,
    ProjectNode,
    HashJoinNode,
    AggregateNode,
    SortNode,
    LimitNode,
    eq,
    gt,
)
from adaptive_engine.optimizer import Optimizer


def make_catalog() -> tuple[Catalog, VolcanoExecutor]:
    catalog = Catalog()
    catalog.create_table(
        "orders",
        [
            {"id": i, "customer_id": i % 5, "amount": i * 10, "status": "open" if i % 3 == 0 else "closed"}
            for i in range(1, 51)
        ],
    )
    catalog.create_table(
        "customers",
        [{"id": i, "name": f"Customer-{i}", "tier": "gold" if i % 2 == 0 else "silver"} for i in range(5)],
    )
    return catalog, VolcanoExecutor(catalog)


def annotate(plan, catalog):
    return Optimizer(catalog).optimize(plan)


class TestScan:
    def test_full_scan(self):
        catalog, exec_ = make_catalog()
        plan = annotate(ScanNode(table="orders"), catalog)
        rows = exec_.execute(plan)
        assert len(rows) == 50

    def test_scan_returns_dicts(self):
        catalog, exec_ = make_catalog()
        plan = annotate(ScanNode(table="orders"), catalog)
        rows = exec_.execute(plan)
        assert isinstance(rows[0], dict)
        assert "id" in rows[0]


class TestFilter:
    def test_filter_reduces_rows(self):
        catalog, exec_ = make_catalog()
        plan = annotate(
            FilterNode(child=ScanNode(table="orders"), predicate=eq("status", "open"), selectivity=0.3),
            catalog,
        )
        rows = exec_.execute(plan)
        assert all(r["status"] == "open" for r in rows)
        assert len(rows) < 50

    def test_filter_gt(self):
        catalog, exec_ = make_catalog()
        plan = annotate(
            FilterNode(child=ScanNode(table="orders"), predicate=gt("amount", 200), selectivity=0.5),
            catalog,
        )
        rows = exec_.execute(plan)
        assert all(r["amount"] > 200 for r in rows)


class TestProject:
    def test_project_keeps_columns(self):
        catalog, exec_ = make_catalog()
        plan = annotate(
            ProjectNode(child=ScanNode(table="orders"), columns=["id", "amount"]),
            catalog,
        )
        rows = exec_.execute(plan)
        assert all(set(r.keys()) == {"id", "amount"} for r in rows)

    def test_project_drops_other_columns(self):
        catalog, exec_ = make_catalog()
        plan = annotate(
            ProjectNode(child=ScanNode(table="orders"), columns=["id"]),
            catalog,
        )
        rows = exec_.execute(plan)
        assert all("status" not in r for r in rows)


class TestHashJoin:
    def test_inner_join(self):
        catalog, exec_ = make_catalog()
        plan = annotate(
            HashJoinNode(
                left=ScanNode(table="orders"),
                right=ScanNode(table="customers"),
                left_key="customer_id",
                right_key="id",
            ),
            catalog,
        )
        rows = exec_.execute(plan)
        assert len(rows) > 0
        assert all("name" in r for r in rows)

    def test_join_key_matches(self):
        catalog, exec_ = make_catalog()
        plan = annotate(
            HashJoinNode(
                left=ScanNode(table="orders"),
                right=ScanNode(table="customers"),
                left_key="customer_id",
                right_key="id",
            ),
            catalog,
        )
        rows = exec_.execute(plan)
        for r in rows:
            assert r["customer_id"] == r["id"]


class TestAggregate:
    def test_count_all(self):
        catalog, exec_ = make_catalog()
        plan = annotate(
            AggregateNode(
                child=ScanNode(table="orders"),
                group_by=[],
                aggregates=[("total", "count", "id")],
            ),
            catalog,
        )
        rows = exec_.execute(plan)
        assert rows[0]["total"] == 50

    def test_sum_by_group(self):
        catalog, exec_ = make_catalog()
        plan = annotate(
            AggregateNode(
                child=ScanNode(table="orders"),
                group_by=["status"],
                aggregates=[("total_amount", "sum", "amount")],
            ),
            catalog,
        )
        rows = exec_.execute(plan)
        statuses = {r["status"] for r in rows}
        assert "open" in statuses
        assert "closed" in statuses


class TestSort:
    def test_sort_ascending(self):
        catalog, exec_ = make_catalog()
        plan = annotate(
            SortNode(child=ScanNode(table="orders"), order_by=[("amount", True)]),
            catalog,
        )
        rows = exec_.execute(plan)
        amounts = [r["amount"] for r in rows]
        assert amounts == sorted(amounts)

    def test_sort_descending(self):
        catalog, exec_ = make_catalog()
        plan = annotate(
            SortNode(child=ScanNode(table="orders"), order_by=[("amount", False)]),
            catalog,
        )
        rows = exec_.execute(plan)
        amounts = [r["amount"] for r in rows]
        assert amounts == sorted(amounts, reverse=True)


class TestLimit:
    def test_limit(self):
        catalog, exec_ = make_catalog()
        plan = annotate(
            LimitNode(child=ScanNode(table="orders"), limit=10),
            catalog,
        )
        rows = exec_.execute(plan)
        assert len(rows) == 10

    def test_limit_with_offset(self):
        catalog, exec_ = make_catalog()
        plan_all = annotate(ScanNode(table="orders"), catalog)
        plan_lim = annotate(LimitNode(child=ScanNode(table="orders"), limit=5, offset=10), catalog)
        all_rows = exec_.execute(plan_all)
        lim_rows = exec_.execute(plan_lim)
        assert lim_rows == all_rows[10:15]
