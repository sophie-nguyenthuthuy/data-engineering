"""Cost model: selectivity estimation and plan cost computation."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from dqp.predicate import (
    AndPredicate,
    BetweenPredicate,
    ColumnRef,
    ComparisonOp,
    ComparisonPredicate,
    InPredicate,
    IsNullPredicate,
    LikePredicate,
    NotPredicate,
    OrPredicate,
    Predicate,
)
from dqp.cost.statistics import StatsRegistry, TableStats


# ---------------------------------------------------------------------------
# Selectivity estimation
# ---------------------------------------------------------------------------


def estimate_selectivity(pred: Predicate, table_stats: TableStats) -> float:
    """Estimate the fraction of rows that satisfy *pred* (0.0 – 1.0)."""

    if isinstance(pred, ComparisonPredicate):
        return _sel_comparison(pred, table_stats)

    if isinstance(pred, BetweenPredicate):
        return _sel_between(pred, table_stats)

    if isinstance(pred, InPredicate):
        return _sel_in(pred, table_stats)

    if isinstance(pred, LikePredicate):
        return _sel_like(pred)

    if isinstance(pred, IsNullPredicate):
        return _sel_is_null(pred, table_stats)

    if isinstance(pred, AndPredicate):
        # Independence assumption: product of selectivities
        result = 1.0
        for child in pred.predicates:
            result *= estimate_selectivity(child, table_stats)
        return result

    if isinstance(pred, OrPredicate):
        # Inclusion-exclusion approximation: 1 - product(1 - sel_i)
        not_sel = 1.0
        for child in pred.predicates:
            not_sel *= 1.0 - estimate_selectivity(child, table_stats)
        return 1.0 - not_sel

    if isinstance(pred, NotPredicate):
        return 1.0 - estimate_selectivity(pred.predicate, table_stats)

    # Unknown predicate type — conservative estimate
    return 0.1


def _col_stats(col: ColumnRef, table_stats: TableStats):
    return table_stats.get_column(col.column)


def _sel_comparison(pred: ComparisonPredicate, ts: TableStats) -> float:
    stats = _col_stats(pred.column, ts)
    val = pred.value.value

    if pred.op == ComparisonOp.EQ:
        if stats and stats.distinct_count > 0:
            return (1.0 - stats.null_fraction) / stats.distinct_count
        return 0.05

    if pred.op == ComparisonOp.NEQ:
        if stats and stats.distinct_count > 0:
            eq_sel = (1.0 - stats.null_fraction) / stats.distinct_count
            return 1.0 - eq_sel
        return 0.95

    # Range: LT, LTE, GT, GTE
    try:
        fval = float(val)
    except (TypeError, ValueError):
        return 0.33

    if not stats:
        return 0.33

    non_null = 1.0 - stats.null_fraction

    if pred.op in (ComparisonOp.LT, ComparisonOp.LTE):
        frac = stats.value_fraction_lt(fval)
        if pred.op == ComparisonOp.LTE and stats.distinct_count > 0:
            # Approximate: add the equality bucket
            frac = min(frac + 1.0 / stats.distinct_count, 1.0)
        return frac * non_null

    if pred.op in (ComparisonOp.GT, ComparisonOp.GTE):
        frac_lt = stats.value_fraction_lt(fval)
        if pred.op == ComparisonOp.GT:
            frac = 1.0 - frac_lt
            # Subtract the equality slice
            if stats.distinct_count > 0:
                frac = max(frac - 1.0 / stats.distinct_count, 0.0)
        else:
            frac = 1.0 - frac_lt
        return frac * non_null

    return 0.33


def _sel_between(pred: BetweenPredicate, ts: TableStats) -> float:
    stats = _col_stats(pred.column, ts)
    try:
        lo = float(pred.low.value)
        hi = float(pred.high.value)
    except (TypeError, ValueError):
        return 0.25 if not pred.negated else 0.75

    non_null = 1.0 - (stats.null_fraction if stats else 0.0)

    if stats:
        frac = stats.value_fraction_between(lo, hi) * non_null
    else:
        # Fallback: assume uniform, 25% range
        frac = 0.25

    return (1.0 - frac) if pred.negated else frac


def _sel_in(pred: InPredicate, ts: TableStats) -> float:
    stats = _col_stats(pred.column, ts)
    n_vals = len(pred.values)
    if n_vals == 0:
        return 0.0 if not pred.negated else 1.0

    if stats and stats.distinct_count > 0:
        sel = min(n_vals / stats.distinct_count, 0.5)
    else:
        sel = min(n_vals * 0.05, 0.5)

    return (1.0 - sel) if pred.negated else sel


def _sel_like(pred: LikePredicate) -> float:
    pattern = pred.pattern
    # Check if pattern starts with a literal prefix (no leading wildcard)
    starts_with_literal = not pattern.startswith("%") and not pattern.startswith("_")
    base = 0.1 if starts_with_literal else 0.3
    return (1.0 - base) if pred.negated else base


def _sel_is_null(pred: IsNullPredicate, ts: TableStats) -> float:
    stats = _col_stats(pred.column, ts)
    null_frac = stats.null_fraction if stats else 0.05
    return (1.0 - null_frac) if pred.negated else null_frac


# ---------------------------------------------------------------------------
# Plan cost
# ---------------------------------------------------------------------------


@dataclass
class PlanCost:
    """Estimated cost of executing a plan node."""

    cpu_cost: float
    io_cost: float
    rows_out: float

    def total(self) -> float:
        """Weighted sum of CPU and IO cost."""
        return self.cpu_cost + self.io_cost

    def __add__(self, other: PlanCost) -> PlanCost:
        return PlanCost(
            self.cpu_cost + other.cpu_cost,
            self.io_cost + other.io_cost,
            max(self.rows_out, other.rows_out),
        )

    def __repr__(self) -> str:
        return (
            f"PlanCost(cpu={self.cpu_cost:.2f}, io={self.io_cost:.2f}, "
            f"rows={self.rows_out:.0f}, total={self.total():.2f})"
        )


# ---------------------------------------------------------------------------
# Cost model
# ---------------------------------------------------------------------------

# IO cost per row, per engine (relative units)
_IO_COST_PER_ROW = {
    "parquet": 0.5,
    "mongodb": 1.0,
    "postgres": 1.0,
}
_DEFAULT_IO_COST = 1.0

# CPU cost per row processed
_CPU_COST_PER_ROW = 0.1
# Overhead of applying a residual Python-side filter
_RESIDUAL_FILTER_OVERHEAD = 0.05


class CostModel:
    """Estimates plan costs using collected statistics."""

    def __init__(self, stats_registry: StatsRegistry) -> None:
        self._registry = stats_registry

    def _get_stats(self, table_name: str) -> Optional[TableStats]:
        return self._registry.get_table_stats(table_name)

    def _row_count(self, table_name: str) -> float:
        stats = self._get_stats(table_name)
        if stats:
            return float(stats.row_count)
        return 100_000.0  # default assumption

    def cost_scan(self, table_name: str, engine_name: str) -> PlanCost:
        """Cost of a full table scan on the given engine."""
        rows = self._row_count(table_name)
        io_per_row = _IO_COST_PER_ROW.get(engine_name.lower(), _DEFAULT_IO_COST)
        io_cost = rows * io_per_row
        cpu_cost = rows * _CPU_COST_PER_ROW
        return PlanCost(cpu_cost=cpu_cost, io_cost=io_cost, rows_out=rows)

    def cost_filter(
        self,
        scan_cost: PlanCost,
        predicate: Predicate,
        table_stats: TableStats,
    ) -> PlanCost:
        """Apply a filter above a scan; reduces rows_out by selectivity."""
        sel = estimate_selectivity(predicate, table_stats)
        rows_in = scan_cost.rows_out
        rows_out = rows_in * sel
        # Extra CPU for evaluating the predicate on each incoming row
        extra_cpu = rows_in * _RESIDUAL_FILTER_OVERHEAD
        return PlanCost(
            cpu_cost=scan_cost.cpu_cost + extra_cpu,
            io_cost=scan_cost.io_cost,
            rows_out=rows_out,
        )

    def cost_pushed_scan(
        self,
        table_name: str,
        engine_name: str,
        pushed_preds: list,
        residual_preds: list,
        table_stats: TableStats,
    ) -> PlanCost:
        """Cost when some predicates are pushed into the engine.

        Engine-native filters reduce IO before rows are shipped;
        residual filters incur additional CPU overhead on the caller.
        """
        rows = self._row_count(table_name)
        io_per_row = _IO_COST_PER_ROW.get(engine_name.lower(), _DEFAULT_IO_COST)

        # Selectivity of pushed predicates (reduces IO)
        if pushed_preds:
            from dqp.predicate import AndPredicate as AP
            combined_pushed = AP(pushed_preds) if len(pushed_preds) > 1 else pushed_preds[0]
            pushed_sel = estimate_selectivity(combined_pushed, table_stats)
        else:
            pushed_sel = 1.0

        rows_after_push = rows * pushed_sel
        io_cost = rows_after_push * io_per_row

        # CPU for evaluating residual predicates in Python
        if residual_preds:
            from dqp.predicate import AndPredicate as AP
            combined_residual = AP(residual_preds) if len(residual_preds) > 1 else residual_preds[0]
            residual_sel = estimate_selectivity(combined_residual, table_stats)
        else:
            residual_sel = 1.0

        rows_after_residual = rows_after_push * residual_sel
        cpu_cost = rows * _CPU_COST_PER_ROW + rows_after_push * _RESIDUAL_FILTER_OVERHEAD

        return PlanCost(
            cpu_cost=cpu_cost,
            io_cost=io_cost,
            rows_out=rows_after_residual,
        )

    def cost_join(
        self,
        left: PlanCost,
        right: PlanCost,
        condition: Predicate,
    ) -> PlanCost:
        """Naive nested-loop join cost estimate."""
        # Assume hash-join: build hash on right side, probe with left
        build_cpu = right.rows_out * _CPU_COST_PER_ROW * 2
        probe_cpu = left.rows_out * _CPU_COST_PER_ROW
        # Selectivity of join condition (rough estimate)
        join_sel = 0.1
        rows_out = left.rows_out * right.rows_out * join_sel
        return PlanCost(
            cpu_cost=left.cpu_cost + right.cpu_cost + build_cpu + probe_cpu,
            io_cost=left.io_cost + right.io_cost,
            rows_out=rows_out,
        )
