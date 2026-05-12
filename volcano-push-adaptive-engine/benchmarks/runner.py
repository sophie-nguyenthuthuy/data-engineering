"""Core benchmark runner.

Times the same query plan under three execution strategies:
  - Volcano-only  (pure pull)
  - Push-only     (pure push pipeline)
  - Adaptive      (starts volcano, switches hot paths to push)

Results are averaged over REPEATS runs to reduce timing noise.
"""
from __future__ import annotations
import copy
import time
from dataclasses import dataclass, field
from typing import Callable

from adaptive_engine import AdaptiveEngine
from adaptive_engine.catalog import Catalog
from adaptive_engine.optimizer import Optimizer
from adaptive_engine.plan import PlanNode
from adaptive_engine.push import PushCompiler
from adaptive_engine.volcano import VolcanoExecutor


REPEATS = 5


@dataclass
class BenchResult:
    scenario: str
    param_name: str
    param_value: object
    n_rows: int
    volcano_ms: float
    push_ms: float
    adaptive_ms: float
    adaptive_mode_switches: int
    adaptive_reopt_rounds: int
    result_rows: int

    @property
    def winner(self) -> str:
        best = min(self.volcano_ms, self.push_ms, self.adaptive_ms)
        if best == self.adaptive_ms:
            return "adaptive"
        if best == self.push_ms:
            return "push"
        return "volcano"

    @property
    def push_speedup(self) -> float:
        return self.volcano_ms / self.push_ms if self.push_ms > 0 else 1.0

    @property
    def adaptive_speedup(self) -> float:
        return self.volcano_ms / self.adaptive_ms if self.adaptive_ms > 0 else 1.0


# A scenario factory returns (catalog, plan) for a given parameter value
ScenarioFactory = Callable[[object], tuple[Catalog, PlanNode]]


@dataclass
class Scenario:
    name: str
    param_name: str
    param_values: list[object]
    factory: ScenarioFactory
    description: str = ""


class BenchmarkRunner:
    def __init__(self, repeats: int = REPEATS) -> None:
        self.repeats = repeats

    def run(self, scenario: Scenario) -> list[BenchResult]:
        results = []
        for val in scenario.param_values:
            catalog, plan = scenario.factory(val)
            result = self._measure(scenario.name, scenario.param_name, val, catalog, plan)
            results.append(result)
        return results

    def _measure(
        self,
        scenario: str,
        param_name: str,
        param_value: object,
        catalog: Catalog,
        plan: PlanNode,
    ) -> BenchResult:
        opt = Optimizer(catalog)

        def _timed(fn: Callable) -> tuple[float, list]:
            rows = []
            times = []
            for _ in range(self.repeats):
                t0 = time.perf_counter()
                rows = fn()
                times.append((time.perf_counter() - t0) * 1000)
            return sum(times) / len(times), rows

        # Volcano
        volcano_ms, volcano_rows = _timed(
            lambda: VolcanoExecutor(catalog).execute(opt.optimize(copy.deepcopy(plan)))
        )

        # Push
        push_ms, push_rows = _timed(
            lambda: PushCompiler(catalog).compile(opt.optimize(copy.deepcopy(plan))).run()
        )

        # Adaptive (well-estimated — no artificial undercount so we see true overhead)
        engine = AdaptiveEngine(catalog, hot_threshold=10.0, check_interval=100)
        adaptive_report_ref = []

        def _run_adaptive():
            rows, report = engine.execute(opt.optimize(copy.deepcopy(plan)))
            adaptive_report_ref.clear()
            adaptive_report_ref.append(report)
            return rows

        adaptive_ms, adaptive_rows = _timed(_run_adaptive)
        report = adaptive_report_ref[0] if adaptive_report_ref else None

        # Determine actual table row count for the label
        n_rows = 0
        for tname in catalog.tables():
            n_rows = max(n_rows, len(catalog.data(tname)))

        return BenchResult(
            scenario=scenario,
            param_name=param_name,
            param_value=param_value,
            n_rows=n_rows,
            volcano_ms=volcano_ms,
            push_ms=push_ms,
            adaptive_ms=adaptive_ms,
            adaptive_mode_switches=len(report.mode_switches) if report else 0,
            adaptive_reopt_rounds=report.reopt_rounds if report else 0,
            result_rows=len(adaptive_rows),
        )
