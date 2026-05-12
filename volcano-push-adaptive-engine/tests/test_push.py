"""Tests for push-based (pipeline) executor."""
import pytest
from adaptive_engine import (
    Catalog,
    PushCompiler,
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


def make_catalog() -> Catalog:
    catalog = Catalog()
    catalog.create_table(
        "products",
        [{"id": i, "category": f"cat{i % 4}", "price": i * 5.0, "in_stock": i % 2 == 0}
         for i in range(1, 41)],
    )
    catalog.create_table(
        "categories",
        [{"id": f"cat{i}", "label": f"Category {i}"} for i in range(4)],
    )
    return catalog


def compile_and_run(catalog, plan):
    plan = Optimizer(catalog).optimize(plan)
    compiler = PushCompiler(catalog)
    pipeline = compiler.compile(plan)
    return pipeline.run()


class TestPushScan:
    def test_full_scan(self):
        catalog = make_catalog()
        rows = compile_and_run(catalog, ScanNode(table="products"))
        assert len(rows) == 40

    def test_scan_contents(self):
        catalog = make_catalog()
        rows = compile_and_run(catalog, ScanNode(table="products"))
        assert all("id" in r and "price" in r for r in rows)


class TestPushFilter:
    def test_filter_in_stock(self):
        catalog = make_catalog()
        from adaptive_engine.expressions import BinOp, ColRef, Literal
        plan = FilterNode(
            child=ScanNode(table="products"),
            predicate=BinOp(ColRef("in_stock"), "=", Literal(True)),
            selectivity=0.5,
        )
        rows = compile_and_run(catalog, plan)
        assert all(r["in_stock"] for r in rows)
        assert len(rows) == 20

    def test_filter_price(self):
        catalog = make_catalog()
        plan = FilterNode(
            child=ScanNode(table="products"),
            predicate=gt("price", 100.0),
            selectivity=0.5,
        )
        rows = compile_and_run(catalog, plan)
        assert all(r["price"] > 100.0 for r in rows)


class TestPushProject:
    def test_project_columns(self):
        catalog = make_catalog()
        plan = ProjectNode(
            child=ScanNode(table="products"),
            columns=["id", "price"],
        )
        rows = compile_and_run(catalog, plan)
        assert all(set(r.keys()) == {"id", "price"} for r in rows)


class TestPushJoin:
    def test_hash_join(self):
        catalog = make_catalog()
        plan = HashJoinNode(
            left=ScanNode(table="products"),
            right=ScanNode(table="categories"),
            left_key="category",
            right_key="id",
        )
        rows = compile_and_run(catalog, plan)
        assert len(rows) > 0
        assert all("label" in r for r in rows)


class TestPushAggregate:
    def test_count_by_category(self):
        catalog = make_catalog()
        plan = AggregateNode(
            child=ScanNode(table="products"),
            group_by=["category"],
            aggregates=[("cnt", "count", "id"), ("total_price", "sum", "price")],
        )
        rows = compile_and_run(catalog, plan)
        assert len(rows) == 4  # 4 categories

    def test_avg(self):
        catalog = make_catalog()
        plan = AggregateNode(
            child=ScanNode(table="products"),
            group_by=[],
            aggregates=[("avg_price", "avg", "price")],
        )
        rows = compile_and_run(catalog, plan)
        assert abs(rows[0]["avg_price"] - 102.5) < 1.0


class TestPushSort:
    def test_sort_by_price(self):
        catalog = make_catalog()
        plan = SortNode(
            child=ScanNode(table="products"),
            order_by=[("price", False)],
        )
        rows = compile_and_run(catalog, plan)
        prices = [r["price"] for r in rows]
        assert prices == sorted(prices, reverse=True)


class TestPushLimit:
    def test_limit(self):
        catalog = make_catalog()
        plan = LimitNode(child=ScanNode(table="products"), limit=7)
        rows = compile_and_run(catalog, plan)
        assert len(rows) == 7


class TestPushPipeline:
    def test_chained_filter_project_sort(self):
        catalog = make_catalog()
        plan = SortNode(
            child=ProjectNode(
                child=FilterNode(
                    child=ScanNode(table="products"),
                    predicate=gt("price", 50.0),
                    selectivity=0.5,
                ),
                columns=["id", "price"],
            ),
            order_by=[("price", True)],
        )
        rows = compile_and_run(catalog, plan)
        assert all(r["price"] > 50.0 for r in rows)
        assert all(set(r.keys()) == {"id", "price"} for r in rows)
        prices = [r["price"] for r in rows]
        assert prices == sorted(prices)
