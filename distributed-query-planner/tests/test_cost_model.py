"""Tests for cost model and selectivity estimation."""
from __future__ import annotations

import pytest

from dqp.cost.model import CostModel, PlanCost, estimate_selectivity
from dqp.cost.statistics import ColumnStats, Histogram, StatsRegistry, TableStats
from dqp.predicate import (
    AndPredicate,
    BetweenPredicate,
    ColumnRef,
    ComparisonOp,
    ComparisonPredicate,
    InPredicate,
    IsNullPredicate,
    LikePredicate,
    Literal,
    NotPredicate,
    OrPredicate,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def col(name: str) -> ColumnRef:
    return ColumnRef(column=name)


def int_lit(v: int) -> Literal:
    return Literal(value=v, dtype="int")


def str_lit(v: str) -> Literal:
    return Literal(value=v, dtype="str")


def make_table_stats(
    row_count: int = 10_000,
    age_ndv: int = 100,
    age_min: float = 0.0,
    age_max: float = 100.0,
    age_null_frac: float = 0.0,
    include_histogram: bool = True,
) -> TableStats:
    """Build a TableStats with an 'age' column and known distribution."""
    boundaries = [float(i * 10) for i in range(11)]  # 0, 10, 20, ..., 100
    frequencies = [0.1] * 10  # uniform

    histogram = Histogram(boundaries=boundaries, frequencies=frequencies) if include_histogram else None

    age_stats = ColumnStats(
        column="age",
        null_fraction=age_null_frac,
        distinct_count=age_ndv,
        min_value=age_min,
        max_value=age_max,
        histogram=histogram,
    )
    status_stats = ColumnStats(
        column="status",
        null_fraction=0.0,
        distinct_count=4,
        min_value=None,
        max_value=None,
        histogram=None,
    )
    nullable_stats = ColumnStats(
        column="deleted_at",
        null_fraction=0.8,
        distinct_count=50,
        min_value=None,
        max_value=None,
        histogram=None,
    )
    return TableStats(
        table_name="orders",
        row_count=row_count,
        column_stats={
            "age": age_stats,
            "status": status_stats,
            "deleted_at": nullable_stats,
        },
    )


# ---------------------------------------------------------------------------
# Histogram tests
# ---------------------------------------------------------------------------


class TestHistogram:
    def test_fraction_lt_boundary_left(self):
        h = Histogram(boundaries=[0.0, 50.0, 100.0], frequencies=[0.5, 0.5])
        assert h.estimate_fraction_lt(0.0) == 0.0

    def test_fraction_lt_boundary_right(self):
        h = Histogram(boundaries=[0.0, 50.0, 100.0], frequencies=[0.5, 0.5])
        assert h.estimate_fraction_lt(100.0) == 1.0

    def test_fraction_lt_midpoint(self):
        h = Histogram(boundaries=[0.0, 50.0, 100.0], frequencies=[0.5, 0.5])
        # At 25 (midpoint of first bucket): 50% of first bucket = 0.5 * 0.5 = 0.25
        frac = h.estimate_fraction_lt(25.0)
        assert abs(frac - 0.25) < 1e-6

    def test_fraction_between(self):
        h = Histogram(boundaries=[0.0, 50.0, 100.0], frequencies=[0.5, 0.5])
        frac = h.estimate_fraction_between(0.0, 50.0)
        assert abs(frac - 0.5) < 0.05

    def test_invalid_histogram_raises(self):
        with pytest.raises(ValueError):
            Histogram(boundaries=[0.0, 50.0], frequencies=[0.3, 0.3, 0.4])

    def test_uniform_10_bucket_histogram(self):
        boundaries = [float(i * 10) for i in range(11)]
        frequencies = [0.1] * 10
        h = Histogram(boundaries=boundaries, frequencies=frequencies)
        # fraction < 50 should be ~0.5
        frac = h.estimate_fraction_lt(50.0)
        assert 0.45 <= frac <= 0.55


# ---------------------------------------------------------------------------
# Selectivity estimation
# ---------------------------------------------------------------------------


class TestSelectivityComparison:
    def test_eq_with_ndv(self):
        ts = make_table_stats(age_ndv=100)
        pred = ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(42))
        sel = estimate_selectivity(pred, ts)
        assert abs(sel - 1.0 / 100) < 1e-9

    def test_eq_no_stats_fallback(self):
        ts = make_table_stats()
        pred = ComparisonPredicate(col("unknown_col"), ComparisonOp.EQ, int_lit(1))
        sel = estimate_selectivity(pred, ts)
        assert sel == 0.05

    def test_neq_complement_of_eq(self):
        ts = make_table_stats(age_ndv=100)
        eq_pred = ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(42))
        neq_pred = ComparisonPredicate(col("age"), ComparisonOp.NEQ, int_lit(42))
        sel_eq = estimate_selectivity(eq_pred, ts)
        sel_neq = estimate_selectivity(neq_pred, ts)
        assert abs(sel_eq + sel_neq - 1.0) < 0.01

    def test_lt_uses_histogram(self):
        ts = make_table_stats(include_histogram=True)
        # age < 50 with uniform histogram should be ~0.5
        pred = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(50))
        sel = estimate_selectivity(pred, ts)
        assert 0.45 <= sel <= 0.55

    def test_gt_complement_of_lte(self):
        ts = make_table_stats()
        lt_pred = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(50))
        gt_pred = ComparisonPredicate(col("age"), ComparisonOp.GTE, int_lit(50))
        sel_lt = estimate_selectivity(lt_pred, ts)
        sel_gt = estimate_selectivity(gt_pred, ts)
        # They should sum to ~1.0
        assert 0.95 <= sel_lt + sel_gt <= 1.05

    def test_range_selectivity_monotonic(self):
        ts = make_table_stats()
        p10 = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(10))
        p30 = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(30))
        p70 = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(70))
        s10 = estimate_selectivity(p10, ts)
        s30 = estimate_selectivity(p30, ts)
        s70 = estimate_selectivity(p70, ts)
        assert s10 < s30 < s70


class TestSelectivityBetween:
    def test_between_full_range(self):
        ts = make_table_stats()
        pred = BetweenPredicate(col("age"), int_lit(0), int_lit(100))
        sel = estimate_selectivity(pred, ts)
        # Full range — should be close to 1.0 (minus nulls, of which there are none)
        assert sel > 0.9

    def test_between_half_range(self):
        ts = make_table_stats()
        pred = BetweenPredicate(col("age"), int_lit(0), int_lit(50))
        sel = estimate_selectivity(pred, ts)
        assert 0.4 <= sel <= 0.6

    def test_not_between(self):
        ts = make_table_stats()
        pred = BetweenPredicate(col("age"), int_lit(0), int_lit(50), negated=True)
        sel = estimate_selectivity(pred, ts)
        # NOT BETWEEN [0,50] → BETWEEN [0,100] complement
        between_sel = estimate_selectivity(
            BetweenPredicate(col("age"), int_lit(0), int_lit(50)), ts
        )
        assert abs(sel - (1.0 - between_sel)) < 0.01


class TestSelectivityIn:
    def test_in_proportional_to_values(self):
        ts = make_table_stats()
        p1 = InPredicate(col("age"), [int_lit(10)])
        p5 = InPredicate(col("age"), [int_lit(i * 10) for i in range(5)])
        s1 = estimate_selectivity(p1, ts)
        s5 = estimate_selectivity(p5, ts)
        assert s5 > s1

    def test_in_capped_at_half(self):
        ts = make_table_stats(age_ndv=4)
        # 10 values in a column with 4 NDV should be capped at 0.5
        vals = [int_lit(i) for i in range(10)]
        pred = InPredicate(col("age"), vals)
        sel = estimate_selectivity(pred, ts)
        assert sel <= 0.5

    def test_not_in_complement(self):
        ts = make_table_stats(age_ndv=100)
        vals = [int_lit(i) for i in range(5)]
        p_in = InPredicate(col("age"), vals)
        p_not_in = InPredicate(col("age"), vals, negated=True)
        s_in = estimate_selectivity(p_in, ts)
        s_not_in = estimate_selectivity(p_not_in, ts)
        assert abs(s_in + s_not_in - 1.0) < 1e-9

    def test_empty_in_list(self):
        ts = make_table_stats()
        pred = InPredicate(col("age"), [])
        sel = estimate_selectivity(pred, ts)
        assert sel == 0.0


class TestSelectivityLike:
    def test_like_with_prefix_lower_selectivity(self):
        ts = make_table_stats()
        pred_prefix = LikePredicate(col("name"), "Alice%")
        sel = estimate_selectivity(pred_prefix, ts)
        assert sel == 0.1

    def test_like_no_prefix_higher_selectivity(self):
        ts = make_table_stats()
        pred_any = LikePredicate(col("name"), "%alice%")
        sel = estimate_selectivity(pred_any, ts)
        assert sel == 0.3

    def test_not_like_complement(self):
        ts = make_table_stats()
        pred = LikePredicate(col("name"), "Alice%")
        pred_not = LikePredicate(col("name"), "Alice%", negated=True)
        s = estimate_selectivity(pred, ts)
        s_not = estimate_selectivity(pred_not, ts)
        assert abs(s + s_not - 1.0) < 1e-9


class TestSelectivityIsNull:
    def test_is_null_uses_null_fraction(self):
        ts = make_table_stats()
        pred = IsNullPredicate(col("deleted_at"))
        sel = estimate_selectivity(pred, ts)
        assert abs(sel - 0.8) < 1e-9

    def test_is_not_null(self):
        ts = make_table_stats()
        pred = IsNullPredicate(col("deleted_at"), negated=True)
        sel = estimate_selectivity(pred, ts)
        assert abs(sel - 0.2) < 1e-9


class TestSelectivityCompound:
    def test_and_product_of_selectivities(self):
        ts = make_table_stats(age_ndv=100)
        a = ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(30))  # sel=0.01
        b = IsNullPredicate(col("deleted_at"), negated=True)               # sel=0.2
        and_pred = AndPredicate([a, b])
        sel = estimate_selectivity(and_pred, ts)
        sel_a = estimate_selectivity(a, ts)
        sel_b = estimate_selectivity(b, ts)
        assert abs(sel - sel_a * sel_b) < 1e-9

    def test_or_inclusion_exclusion(self):
        ts = make_table_stats(age_ndv=100)
        a = ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(30))
        b = IsNullPredicate(col("deleted_at"))
        or_pred = OrPredicate([a, b])
        sel = estimate_selectivity(or_pred, ts)
        sel_a = estimate_selectivity(a, ts)
        sel_b = estimate_selectivity(b, ts)
        expected = 1 - (1 - sel_a) * (1 - sel_b)
        assert abs(sel - expected) < 1e-9

    def test_not_selectivity(self):
        ts = make_table_stats(age_ndv=100)
        a = ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(30))
        not_pred = NotPredicate(a)
        sel = estimate_selectivity(not_pred, ts)
        sel_a = estimate_selectivity(a, ts)
        assert abs(sel - (1.0 - sel_a)) < 1e-9

    def test_and_three_predicates(self):
        ts = make_table_stats(age_ndv=100)
        preds = [
            ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(30)),
            IsNullPredicate(col("deleted_at"), negated=True),
            InPredicate(col("status"), [str_lit("active")]),
        ]
        and_pred = AndPredicate(preds)
        sel = estimate_selectivity(and_pred, ts)
        expected = 1.0
        for p in preds:
            expected *= estimate_selectivity(p, ts)
        assert abs(sel - expected) < 1e-9


# ---------------------------------------------------------------------------
# CostModel
# ---------------------------------------------------------------------------


class TestCostModel:
    def setup_method(self):
        self.registry = StatsRegistry()
        ts = make_table_stats(row_count=100_000)
        self.registry.set_table_stats(ts)
        self.model = CostModel(self.registry)

    def test_cost_scan_parquet_cheaper_io(self):
        parquet_cost = self.model.cost_scan("orders", "parquet")
        mongo_cost = self.model.cost_scan("orders", "mongodb")
        assert parquet_cost.io_cost < mongo_cost.io_cost

    def test_cost_scan_rows_match_stats(self):
        cost = self.model.cost_scan("orders", "parquet")
        assert cost.rows_out == 100_000

    def test_cost_filter_reduces_rows(self):
        ts = self.registry.get_table_stats("orders")
        scan_cost = self.model.cost_scan("orders", "parquet")
        # Filter to age = 42 (selectivity = 1/100)
        pred = ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(42))
        filtered = self.model.cost_filter(scan_cost, pred, ts)
        assert filtered.rows_out < scan_cost.rows_out
        expected_rows = scan_cost.rows_out * (1.0 / 100)
        assert abs(filtered.rows_out - expected_rows) < 1.0

    def test_cost_pushed_scan_less_io_than_full_scan(self):
        ts = self.registry.get_table_stats("orders")
        pred = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(10))
        full = self.model.cost_scan("orders", "parquet")
        pushed = self.model.cost_pushed_scan("orders", "parquet", [pred], [], ts)
        assert pushed.io_cost < full.io_cost

    def test_cost_pushed_scan_no_pushed_equals_full_scan_io(self):
        ts = self.registry.get_table_stats("orders")
        pushed = self.model.cost_pushed_scan("orders", "parquet", [], [], ts)
        full = self.model.cost_scan("orders", "parquet")
        assert pushed.io_cost == full.io_cost

    def test_plan_cost_total(self):
        cost = PlanCost(cpu_cost=100.0, io_cost=200.0, rows_out=1000.0)
        assert cost.total() == 300.0

    def test_plan_cost_add(self):
        a = PlanCost(100.0, 200.0, 1000.0)
        b = PlanCost(50.0, 100.0, 500.0)
        combined = a + b
        assert combined.cpu_cost == 150.0
        assert combined.io_cost == 300.0

    def test_cost_join(self):
        left = PlanCost(100.0, 200.0, 1000.0)
        right = PlanCost(50.0, 100.0, 500.0)
        pred = ComparisonPredicate(col("id"), ComparisonOp.EQ, int_lit(1))
        join_cost = self.model.cost_join(left, right, pred)
        assert join_cost.total() > left.total()
        assert join_cost.rows_out > 0
