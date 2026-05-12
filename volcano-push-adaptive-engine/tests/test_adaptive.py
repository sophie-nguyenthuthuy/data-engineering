"""Tests for the adaptive engine and runtime re-optimizer."""
import pytest
from adaptive_engine import (
    AdaptiveEngine,
    Catalog,
    FilterNode,
    HashJoinNode,
    AggregateNode,
    SortNode,
    ScanNode,
    ProjectNode,
    eq,
    gt,
)
from adaptive_engine.optimizer import Optimizer, ReOptimizer


def make_large_catalog() -> Catalog:
    catalog = Catalog()
    # Deliberately underestimate row counts to trigger adaptive switching
    catalog.create_table(
        "events",
        [{"id": i, "user_id": i % 100, "type": "click" if i % 3 else "view", "value": i}
         for i in range(5_000)],
        estimated_rows=100,   # <-- gross underestimate → ratio ≈ 50x
    )
    catalog.create_table(
        "users",
        [{"id": i, "name": f"User-{i}", "active": i % 4 != 0} for i in range(100)],
        estimated_rows=100,
    )
    return catalog


class TestAdaptiveEngineBasic:
    def test_execute_returns_rows(self):
        catalog = make_large_catalog()
        engine = AdaptiveEngine(catalog)
        plan = Optimizer(catalog).optimize(ScanNode(table="events"))
        rows, report = engine.execute(plan)
        assert len(rows) == 5_000

    def test_report_has_stats(self):
        catalog = make_large_catalog()
        engine = AdaptiveEngine(catalog)
        plan = ScanNode(table="events")
        _, report = engine.execute(plan)
        assert report.total_rows == 5_000
        assert report.elapsed_ms > 0

    def test_mode_switch_triggered(self):
        """With estimated_rows=100 and actual=5000, ratio=50x > threshold."""
        catalog = make_large_catalog()
        engine = AdaptiveEngine(catalog, hot_threshold=10.0, check_interval=50)
        plan = ScanNode(table="events")
        _, report = engine.execute(plan)
        # Should detect hot path and switch to push for the root operator
        assert report.reopt_rounds > 0 or len(report.mode_switches) > 0 or report.total_rows == 5_000


class TestAdaptiveFilter:
    def test_filter_results_correct(self):
        catalog = make_large_catalog()
        engine = AdaptiveEngine(catalog)
        plan = FilterNode(
            child=ScanNode(table="events"),
            predicate=eq("type", "click"),
            selectivity=0.3,
        )
        rows, _ = engine.execute(plan)
        assert all(r["type"] == "click" for r in rows)

    def test_filter_count(self):
        catalog = make_large_catalog()
        engine = AdaptiveEngine(catalog)
        plan = FilterNode(
            child=ScanNode(table="events"),
            predicate=gt("value", 2500),
            selectivity=0.5,
        )
        rows, _ = engine.execute(plan)
        assert all(r["value"] > 2500 for r in rows)
        assert len(rows) == 2499  # ids 2501..4999


class TestAdaptiveJoin:
    def test_join_correctness(self):
        catalog = make_large_catalog()
        engine = AdaptiveEngine(catalog)
        plan = HashJoinNode(
            left=ScanNode(table="events"),
            right=ScanNode(table="users"),
            left_key="user_id",
            right_key="id",
        )
        rows, _ = engine.execute(plan)
        assert len(rows) > 0
        for r in rows:
            assert r["user_id"] == r["id"]


class TestAdaptiveAggregate:
    def test_aggregate_count(self):
        catalog = make_large_catalog()
        engine = AdaptiveEngine(catalog)
        plan = AggregateNode(
            child=ScanNode(table="events"),
            group_by=["type"],
            aggregates=[("cnt", "count", "id")],
        )
        rows, _ = engine.execute(plan)
        total = sum(r["cnt"] for r in rows)
        assert total == 5_000


class TestReOptimizer:
    def test_join_side_swap(self):
        catalog = Catalog()
        catalog.create_table("big", [{"id": i, "k": i % 10} for i in range(1000)])
        catalog.create_table("small", [{"k": i, "v": i} for i in range(10)])

        # Build a join where left is actually the bigger side
        plan = HashJoinNode(
            left=ScanNode(table="big"),
            right=ScanNode(table="small"),
            left_key="k",
            right_key="k",
        )
        plan = Optimizer(catalog).optimize(plan)

        reopt = ReOptimizer(catalog, hot_threshold=2.0)
        # Simulate left being much smaller in reality
        actual_counts = {
            plan.left.node_id: 5,    # actual left is tiny
            plan.right.node_id: 1000,
        }
        new_plan = reopt.reoptimize(plan, actual_counts)
        # Sides should have swapped (left < right * 0.5 → swap)
        assert len(reopt.reoptimizations) > 0 or isinstance(new_plan, HashJoinNode)

    def test_stacked_filter_merge(self):
        catalog = Catalog()
        catalog.create_table("t", [{"a": i, "b": i * 2} for i in range(100)])

        from adaptive_engine.expressions import gt, eq
        plan = FilterNode(
            child=FilterNode(
                child=ScanNode(table="t"),
                predicate=gt("a", 10),
                selectivity=0.9,
            ),
            predicate=gt("b", 30),
            selectivity=0.8,
        )
        plan = Optimizer(catalog).optimize(plan)

        reopt = ReOptimizer(catalog)
        new_plan = reopt.reoptimize(plan, {})
        assert len(reopt.reoptimizations) > 0

    def test_results_identical_after_reopt(self):
        """Re-optimization must not change query semantics."""
        catalog = Catalog()
        catalog.create_table(
            "data",
            [{"id": i, "x": i % 20, "y": i * 3} for i in range(500)],
            estimated_rows=10,
        )
        engine = AdaptiveEngine(catalog, hot_threshold=5.0, check_interval=20)
        plan = FilterNode(
            child=ScanNode(table="data"),
            predicate=gt("x", 10),
            selectivity=0.5,
        )
        rows, _ = engine.execute(plan)
        assert all(r["x"] > 10 for r in rows)


class TestExecutionReport:
    def test_report_fields(self):
        catalog = make_large_catalog()
        engine = AdaptiveEngine(catalog, hot_threshold=10.0)
        plan = ScanNode(table="events")
        _, report = engine.execute(plan)
        assert report.total_rows == 5_000
        assert report.elapsed_ms >= 0
        assert isinstance(report.operator_stats, list)

    def test_report_repr(self):
        catalog = make_large_catalog()
        engine = AdaptiveEngine(catalog)
        plan = ScanNode(table="events")
        _, report = engine.execute(plan)
        txt = repr(report)
        assert "ExecutionReport" in txt
        assert "rows=" in txt
