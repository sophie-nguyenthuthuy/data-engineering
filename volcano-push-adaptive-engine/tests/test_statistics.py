"""Tests for equi-depth histogram statistics."""
import pytest
from adaptive_engine.statistics import EquiDepthHistogram


class TestEquiDepthHistogram:
    def test_basic_construction(self):
        vals = list(range(100))
        h = EquiDepthHistogram(vals, n_buckets=10)
        assert len(h.buckets) == 10
        assert h.total_rows == 100

    def test_empty(self):
        h = EquiDepthHistogram([])
        assert h.total_rows == 0
        assert h.selectivity_eq(5) == 0.0
        assert h.selectivity_range(0, 10) == 0.0

    def test_null_count(self):
        h = EquiDepthHistogram([1, 2, None, 3, None], n_buckets=5)
        assert h.null_count == 2
        assert h.total_rows == 3

    def test_selectivity_eq_uniform(self):
        vals = list(range(100))
        h = EquiDepthHistogram(vals, n_buckets=10)
        # Each distinct value in a 10-bucket, 100-value histogram: ~1/100
        sel = h.selectivity_eq(50)
        assert 0.0 < sel <= 0.05

    def test_selectivity_eq_skewed(self):
        # 90 copies of value 1, 10 copies of value 2
        vals = [1] * 90 + [2] * 10
        h = EquiDepthHistogram(vals, n_buckets=10)
        # Value 1 is in the dense bucket; should have higher selectivity
        sel1 = h.selectivity_eq(1)
        sel2 = h.selectivity_eq(2)
        assert sel1 > sel2

    def test_selectivity_range_full(self):
        vals = list(range(100))
        h = EquiDepthHistogram(vals, n_buckets=10)
        assert h.selectivity_range(0, 99) == pytest.approx(1.0, abs=0.05)

    def test_selectivity_range_half(self):
        vals = list(range(100))
        h = EquiDepthHistogram(vals, n_buckets=10)
        sel = h.selectivity_range(0, 49)
        assert 0.4 <= sel <= 0.6

    def test_selectivity_range_empty(self):
        vals = list(range(100))
        h = EquiDepthHistogram(vals, n_buckets=10)
        # Range outside all values
        assert h.selectivity_range(200, 300) == 0.0

    def test_selectivity_ne(self):
        vals = list(range(100))
        h = EquiDepthHistogram(vals, n_buckets=10)
        sel_eq = h.selectivity_eq(50)
        sel_ne = h.selectivity_ne(50)
        assert abs(sel_eq + sel_ne - 1.0) < 1e-9

    def test_selectivity_lt(self):
        vals = list(range(100))
        h = EquiDepthHistogram(vals, n_buckets=10)
        sel = h.selectivity_lt(50)
        assert 0.3 <= sel <= 0.7

    def test_selectivity_gt(self):
        vals = list(range(100))
        h = EquiDepthHistogram(vals, n_buckets=10)
        sel_lt = h.selectivity_lt(50)
        sel_gt = h.selectivity_gt(50)
        # lt + gt should sum to roughly 1 (with some double-counting at boundary)
        assert sel_lt + sel_gt <= 1.1

    def test_for_op_dispatch(self):
        vals = list(range(1000))
        h = EquiDepthHistogram(vals, n_buckets=20)
        assert h.for_op("=", 500) == h.selectivity_eq(500)
        assert h.for_op("!=", 500) == h.selectivity_ne(500)
        assert h.for_op("<", 500) == h.selectivity_lt(500)
        assert h.for_op(">", 500) == h.selectivity_gt(500)
        assert h.for_op("<=", 500) == h.selectivity_lte(500)
        assert h.for_op(">=", 500) == h.selectivity_gte(500)
        assert h.for_op("LIKE", 500) == 0.5  # unknown op → default

    def test_min_max(self):
        vals = list(range(50, 150))
        h = EquiDepthHistogram(vals, n_buckets=10)
        assert h.min_val == 50
        assert h.max_val == 149

    def test_float_values(self):
        import random
        rng = random.Random(0)
        vals = [rng.uniform(0, 100) for _ in range(1000)]
        h = EquiDepthHistogram(vals, n_buckets=20)
        assert len(h.buckets) == 20
        sel = h.selectivity_range(40.0, 60.0)
        assert 0.1 <= sel <= 0.4

    def test_string_values_unsortable_fallback(self):
        # Mixed types that can't be sorted — should gracefully fall back
        vals = [1, "a", 2, "b"]
        h = EquiDepthHistogram(vals, n_buckets=4)
        # Should not raise; may have no buckets
        assert h.total_rows == 4


class TestHistogramInCatalog:
    def test_catalog_builds_histograms(self):
        from adaptive_engine import Catalog
        catalog = Catalog()
        data = [{"x": i, "y": i * 2} for i in range(200)]
        ts = catalog.create_table("t", data)
        col = ts.column("x")
        assert col is not None
        assert col.histogram is not None
        assert col.histogram.total_rows == 200

    def test_catalog_no_histogram(self):
        from adaptive_engine import Catalog
        catalog = Catalog()
        data = [{"x": i} for i in range(100)]
        ts = catalog.create_table("t", data, build_histograms=False)
        col = ts.column("x")
        assert col is not None
        assert col.histogram is None

    def test_selectivity_for_op_uses_histogram(self):
        from adaptive_engine import Catalog
        catalog = Catalog()
        data = [{"x": i} for i in range(1000)]
        ts = catalog.create_table("t", data)
        col = ts.column("x")
        assert col is not None
        sel = col.selectivity_for_op("=", 500)
        # Histogram-derived: should be ~1/1000, not 1/distinct
        assert sel < 0.01

    def test_optimizer_uses_histogram_selectivity(self):
        from adaptive_engine import Catalog, FilterNode, ScanNode, gt
        from adaptive_engine.optimizer import Optimizer
        catalog = Catalog()
        # Uniform 0..999: P(x > 900) ≈ 10%
        data = [{"x": i} for i in range(1000)]
        catalog.create_table("t", data)
        plan = FilterNode(
            child=ScanNode(table="t"),
            predicate=gt("x", 900),
            selectivity=0.9,  # deliberately wrong guess
        )
        Optimizer(catalog).optimize(plan)
        # After optimization the selectivity should be close to 10%, not 90%
        assert plan.selectivity < 0.2
