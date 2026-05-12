"""Adaptive execution engine.

Starts every query in Volcano (pull) mode with a lightweight profiling
wrapper around each operator.  As rows are consumed:

* If any operator's *actual* cardinality exceeds its estimate by
  HOT_THRESHOLD (default 10×), the engine intercepts execution and
  switches that subtree to Push mode.

* If estimates are still wrong after the push-mode run, the
  ReOptimizer rewrites the plan and re-executes (at most
  MAX_REOPT_ROUNDS times to prevent runaway loops).

Public API
----------
    engine = AdaptiveEngine(catalog)
    result, report = engine.execute(plan)

The returned `report` is an ExecutionReport with timing, mode switches,
and re-optimizations applied.
"""
from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import Iterator

from .catalog import Catalog
from .expressions import Row
from .optimizer import Optimizer, ReOptimizer
from .plan import BufferNode, PlanNode, plan_repr, walk
from .profiler import HotPathSignal, OperatorStats, QueryProfiler
from .push import PushCompiler
from .volcano import VolcanoExecutor


# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

HOT_THRESHOLD: float = 10.0      # ratio actual/estimated that triggers a mode switch
CHECK_INTERVAL: int = 100         # rows between ratio checks
MAX_REOPT_ROUNDS: int = 3         # max plan rewrites per query


# ------------------------------------------------------------------
# Report
# ------------------------------------------------------------------

@dataclass
class ModeSwitch:
    node_id: str
    at_row: int
    ratio: float
    from_mode: str
    to_mode: str

    def __repr__(self) -> str:
        return (
            f"ModeSwitch({self.node_id}: {self.from_mode}→{self.to_mode} "
            f"after {self.at_row} rows, ratio={self.ratio:.1f}x)"
        )


@dataclass
class ExecutionReport:
    total_rows: int = 0
    elapsed_ms: float = 0.0
    mode_switches: list[ModeSwitch] = field(default_factory=list)
    reoptimizations: list[str] = field(default_factory=list)
    operator_stats: list[OperatorStats] = field(default_factory=list)
    reopt_rounds: int = 0
    final_plan_repr: str = ""

    def __repr__(self) -> str:
        lines = [
            "=== ExecutionReport ===",
            f"  rows={self.total_rows}  elapsed={self.elapsed_ms:.2f}ms  "
            f"reopt_rounds={self.reopt_rounds}",
        ]
        if self.mode_switches:
            lines.append("  Mode switches:")
            for ms in self.mode_switches:
                lines.append(f"    {ms}")
        if self.reoptimizations:
            lines.append("  Re-optimizations:")
            for r in self.reoptimizations:
                lines.append(f"    • {r}")
        lines.append("  Operator stats:")
        for s in self.operator_stats:
            lines.append(f"    {s}")
        return "\n".join(lines)


# ------------------------------------------------------------------
# Adaptive engine
# ------------------------------------------------------------------

class AdaptiveEngine:
    """Executes a query plan, adaptively switching between Volcano and Push."""

    def __init__(
        self,
        catalog: Catalog,
        hot_threshold: float = HOT_THRESHOLD,
        check_interval: int = CHECK_INTERVAL,
        max_reopt_rounds: int = MAX_REOPT_ROUNDS,
    ) -> None:
        self.catalog = catalog
        self.hot_threshold = hot_threshold
        self.check_interval = check_interval
        self.max_reopt_rounds = max_reopt_rounds

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def execute(self, plan: PlanNode) -> tuple[list[Row], ExecutionReport]:
        report = ExecutionReport()
        t0 = time.perf_counter()

        # Annotate plan with estimates and node IDs
        plan = Optimizer(self.catalog).optimize(plan)
        report.final_plan_repr = plan_repr(plan)

        rows = self._execute_adaptive(plan, report)

        report.total_rows = len(rows)
        report.elapsed_ms = (time.perf_counter() - t0) * 1_000
        return rows, report

    # ------------------------------------------------------------------
    # Adaptive execution loop
    # ------------------------------------------------------------------

    def _execute_adaptive(
        self, plan: PlanNode, report: ExecutionReport
    ) -> list[Row]:
        profiler = QueryProfiler(
            hot_threshold=self.hot_threshold,
            check_interval=self.check_interval,
        )
        volcano = VolcanoExecutor(self.catalog)
        reopt = ReOptimizer(self.catalog, hot_threshold=self.hot_threshold)

        rows: list[Row] = []
        round_num = 0

        while True:
            rows, hot_signal = self._drain_with_profiler(
                volcano.iter(plan), plan, profiler, report
            )

            report.operator_stats = profiler.all_stats()

            if not hot_signal or round_num >= self.max_reopt_rounds:
                break

            round_num += 1
            report.reopt_rounds += 1

            # --- Switch the hot subtree to push mode ---
            hot_node = _find_node(plan, hot_signal.node_id)
            if hot_node is None:
                break

            report.mode_switches.append(
                ModeSwitch(
                    node_id=hot_signal.node_id,
                    at_row=hot_signal.stats.actual_rows,
                    ratio=hot_signal.stats.cardinality_ratio,
                    from_mode="volcano",
                    to_mode="push",
                )
            )

            push_rows = self._run_push(hot_node, report)

            # Replace the hot subtree with a BufferNode containing push results
            plan = _replace_node(
                plan,
                hot_signal.node_id,
                BufferNode(
                    rows=push_rows,
                    estimated_rows=len(push_rows),
                    source_repr=repr(hot_node),
                ),
            )

            # --- Re-optimize the remaining plan ---
            actual_counts = {s.node_id: s.actual_rows for s in profiler.all_stats()}
            plan = reopt.reoptimize(plan, actual_counts)
            report.reoptimizations.extend(reopt.reoptimizations)

            # Reset profiler for the next round
            profiler = QueryProfiler(
                hot_threshold=self.hot_threshold,
                check_interval=self.check_interval,
            )

        return rows

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _drain_with_profiler(
        self,
        iterator: Iterator[Row],
        plan: PlanNode,
        profiler: QueryProfiler,
        report: ExecutionReport,
    ) -> tuple[list[Row], HotPathSignal | None]:
        """Drain *iterator* through a profiling wrapper.

        Returns (rows_so_far, signal_or_None).
        """
        wrapped = profiler.wrap(iterator, plan, raise_on_hot=True)
        rows: list[Row] = []
        signal: HotPathSignal | None = None
        try:
            for row in wrapped:
                rows.append(row)
        except HotPathSignal as sig:
            signal = sig
            # Drain the rest without raising further signals
            for row in profiler.finalize(iterator, plan):
                rows.append(row)
        except StopIteration:
            pass
        return rows, signal

    def _run_push(self, node: PlanNode, report: ExecutionReport) -> list[Row]:
        compiler = PushCompiler(self.catalog)
        pipeline = compiler.compile(node)
        return pipeline.run()


# ------------------------------------------------------------------
# Plan-tree surgery utilities
# ------------------------------------------------------------------

def _find_node(root: PlanNode, node_id: str) -> PlanNode | None:
    for n in walk(root):
        if n.node_id == node_id:
            return n
    return None


def _replace_node(root: PlanNode, node_id: str, replacement: PlanNode) -> PlanNode:
    """Return a new plan tree with the node matching *node_id* replaced."""
    if root.node_id == node_id:
        return replacement

    # Replace in child references
    for attr in ("child", "left", "right"):
        child = getattr(root, attr, None)
        if isinstance(child, PlanNode):
            new_child = _replace_node(child, node_id, replacement)
            if new_child is not child:
                setattr(root, attr, new_child)

    return root
