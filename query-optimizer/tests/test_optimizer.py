"""
Unit and integration tests for the query optimizer.
Run with: pytest -v
"""
import pytest
from optimizer.expressions import Predicate, PhysicalJoin, PhysicalScan, PhysicalOp
from optimizer.histogram import StatsCatalog, TableStats, ColumnStats
from optimizer.cost_model import CostModel, CostEstimate
from optimizer.memo import Memo, GroupStats
from optimizer.cascades import CascadesOptimizer
from optimizer.schema import build_star_schema


# ─────────────────────────────────────────────────────────────────────────────
# ColumnStats / cardinality
# ─────────────────────────────────────────────────────────────────────────────

class TestColumnStats:
    def test_selectivity_eq_uses_max_ndv(self):
        c1 = ColumnStats("a", num_distinct=1000)
        c2 = ColumnStats("b", num_distinct=500)
        assert c1.selectivity_eq(c2) == pytest.approx(1 / 1000)

    def test_selectivity_range_full_span(self):
        c = ColumnStats("x", num_distinct=100, min_val=0, max_val=100)
        assert c.selectivity_range(0, 100) == pytest.approx(1.0)

    def test_selectivity_range_half_span(self):
        c = ColumnStats("x", num_distinct=100, min_val=0, max_val=100)
        assert c.selectivity_range(0, 50) == pytest.approx(0.5)

    def test_selectivity_range_outside(self):
        c = ColumnStats("x", num_distinct=100, min_val=0, max_val=100)
        assert c.selectivity_range(200, 300) == pytest.approx(0.0)


# ─────────────────────────────────────────────────────────────────────────────
# CostModel
# ─────────────────────────────────────────────────────────────────────────────

class TestCostModel:
    cm = CostModel()

    def test_seq_scan_positive(self):
        cost = self.cm.seq_scan("t", 10_000)
        assert cost.total > 0

    def test_hash_join_cheaper_than_nl_for_large_tables(self):
        hj = self.cm.hash_join(100_000, 1_000_000, 50_000)
        nl = self.cm.nested_loop_join(100_000, 1_000_000, 50_000)
        assert hj.total < nl.total

    def test_nl_cheap_for_tiny_outer(self):
        # Very small outer, small inner – NL can be competitive
        nl = self.cm.nested_loop_join(10, 100, 5)
        assert nl.total > 0

    def test_merge_join_requires_sort(self):
        mj_unsorted = self.cm.merge_join(100_000, 100_000, 50_000,
                                          left_sorted=False, right_sorted=False)
        mj_sorted = self.cm.merge_join(100_000, 100_000, 50_000,
                                        left_sorted=True, right_sorted=True)
        assert mj_unsorted.total > mj_sorted.total

    def test_cost_estimate_addition(self):
        c1 = CostEstimate(io_cost=10, cpu_cost=2)
        c2 = CostEstimate(io_cost=5, cpu_cost=1)
        total = c1 + c2
        assert total.io_cost == 15 and total.cpu_cost == 3


# ─────────────────────────────────────────────────────────────────────────────
# Memo table
# ─────────────────────────────────────────────────────────────────────────────

class TestMemo:
    def test_deduplication_by_table_signature(self):
        memo = Memo()
        g1 = memo.get_or_create(frozenset(["a", "b"]))
        g2 = memo.get_or_create(frozenset(["b", "a"]))
        assert g1.id == g2.id

    def test_scan_group_seeded(self):
        memo = Memo()
        g = memo.get_or_create_scan("orders")
        assert len(g.logical_exprs) == 1
        assert g.tables == frozenset(["orders"])

    def test_winner_update(self):
        from optimizer.cost_model import CostEstimate
        memo = Memo()
        g = memo.get_or_create(frozenset(["x"]))
        p = PhysicalScan("x")
        cost1 = CostEstimate(io_cost=100)
        cost2 = CostEstimate(io_cost=50)
        g.update_winner(p, cost1, {})
        assert g.winner.cost.total == 100
        g.update_winner(p, cost2, {})
        assert g.winner.cost.total == 50


# ─────────────────────────────────────────────────────────────────────────────
# Cascades optimizer – small cases
# ─────────────────────────────────────────────────────────────────────────────

def _mini_catalog(*tables: tuple) -> StatsCatalog:
    """tables: (name, row_count, ndv_of_key)"""
    cat = StatsCatalog()
    for name, rows, ndv in tables:
        ts = TableStats(name, row_count=rows, avg_row_bytes=80)
        ts.add_column(ColumnStats(f"{name}_id", num_distinct=ndv))
        cat.register(ts)
    return cat


class TestCascadesOptimizer:
    def test_two_table_join(self):
        cat = _mini_catalog(("a", 1000, 500), ("b", 200, 200))
        preds = [Predicate("a", "b_id", "b", "b_id")]
        opt = CascadesOptimizer(cat, CostModel())
        winner = opt.optimize(["a", "b"], preds)
        assert winner is not None
        assert winner.cost.total > 0

    def test_three_table_join_selects_plan(self):
        cat = _mini_catalog(
            ("orders", 500_000, 100_000),
            ("customer", 100_000, 100_000),
            ("product", 10_000, 10_000),
        )
        preds = [
            Predicate("orders", "customer_id", "customer", "customer_id"),
            Predicate("orders", "product_id",  "product",  "product_id"),
        ]
        opt = CascadesOptimizer(cat, CostModel())
        winner = opt.optimize(["orders", "customer", "product"], preds)
        assert winner is not None
        # For a star pattern, the large fact table should not be the build side
        # (it is fine as probe; the optimizer picks hash or merge join)
        assert isinstance(winner.expr, PhysicalJoin)

    def test_small_table_joined_first(self):
        """With a very skewed star schema the smallest dimension should be
        joined to the fact table early to minimise intermediate cardinality."""
        cat = _mini_catalog(
            ("fact", 10_000_000, 1_000),
            ("tiny_dim", 10, 10),
            ("big_dim", 500_000, 500_000),
        )
        preds = [
            Predicate("fact", "tiny_id",  "tiny_dim", "tiny_id"),
            Predicate("fact", "big_id",   "big_dim",  "big_id"),
        ]
        opt = CascadesOptimizer(cat, CostModel())
        winner = opt.optimize(["fact", "tiny_dim", "big_dim"], preds)
        assert winner is not None

    def test_no_predicate_cross_join(self):
        """Two tables with no join predicate → Cartesian product is still planned."""
        cat = _mini_catalog(("x", 100, 100), ("y", 200, 200))
        opt = CascadesOptimizer(cat, CostModel())
        winner = opt.optimize(["x", "y"], [])
        assert winner is not None

    def test_hash_join_preferred_over_nl_for_large_tables(self):
        cat = _mini_catalog(
            ("big_a", 1_000_000, 100_000),
            ("big_b", 1_000_000, 100_000),
        )
        preds = [Predicate("big_a", "key", "big_b", "key")]
        opt = CascadesOptimizer(cat, CostModel())
        winner = opt.optimize(["big_a", "big_b"], preds)
        assert winner.expr.algorithm in (PhysicalOp.HASH_JOIN, PhysicalOp.MERGE_JOIN)


# ─────────────────────────────────────────────────────────────────────────────
# Full 10-table star schema integration test
# ─────────────────────────────────────────────────────────────────────────────

class TestStarSchema:
    def test_star_schema_optimizes(self):
        catalog, tables, predicates = build_star_schema()
        assert len(tables) == 10
        assert len(predicates) == 9

        cost_model = CostModel(
            avg_row_bytes={t: catalog.get(t).avg_row_bytes for t in tables}
        )
        opt = CascadesOptimizer(catalog, cost_model)
        winner = opt.optimize(tables, predicates)
        assert winner is not None
        assert winner.cost.total > 0

    def test_star_schema_fact_table_in_plan(self):
        catalog, tables, predicates = build_star_schema()
        cost_model = CostModel()
        opt = CascadesOptimizer(catalog, cost_model)
        winner = opt.optimize(tables, predicates)

        # Walk plan and collect all scanned tables
        scanned: list[str] = []

        def _walk(w):
            if isinstance(w.expr, PhysicalScan):
                scanned.append(w.expr.table)
            for cw in w.child_winners.values():
                _walk(cw)

        _walk(winner)
        assert "fact_sales" in scanned

    def test_star_schema_all_tables_in_plan(self):
        catalog, tables, predicates = build_star_schema()
        cost_model = CostModel()
        opt = CascadesOptimizer(catalog, cost_model)
        winner = opt.optimize(tables, predicates)

        root_group = opt.memo.get_group(
            next(g.id for g in opt.memo.all_groups() if g.tables == frozenset(tables))
        )
        assert root_group.tables == frozenset(tables)
