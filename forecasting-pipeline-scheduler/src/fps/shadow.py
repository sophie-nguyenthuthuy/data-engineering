"""Shadow-mode regret accounting.

For a single DAG :func:`regret` returns the difference between the
baseline FCFS makespan and the list-scheduler's makespan, along with
the speedup ratio.

For a workload of many DAGs :func:`regret_over_dags` returns the
aggregate (mean / median / p95) regret. Negative aggregate regret
means the smarter scheduler is *worse* — the contract is "ship only
when aggregate regret is comfortably above zero".
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from fps.scheduler.baseline import baseline_fcfs_schedule
from fps.scheduler.common import makespan
from fps.scheduler.list_sched import list_schedule

if TYPE_CHECKING:
    from fps.dag import DAG


@dataclass(frozen=True, slots=True)
class RegretReport:
    """Per-DAG (or aggregate) regret summary."""

    baseline_makespan: float
    our_makespan: float
    regret: float
    speedup: float

    @classmethod
    def from_makespans(cls, baseline: float, ours: float) -> RegretReport:
        speedup = baseline / ours if ours > 0 else 1.0
        return cls(
            baseline_makespan=baseline,
            our_makespan=ours,
            regret=baseline - ours,
            speedup=speedup,
        )


def regret(dag: DAG, num_workers: int = 2) -> RegretReport:
    """Single-DAG regret of list_schedule vs baseline_fcfs_schedule."""
    base = baseline_fcfs_schedule(dag, num_workers)
    ours = list_schedule(dag, num_workers)
    return RegretReport.from_makespans(makespan(base), makespan(ours))


@dataclass(frozen=True, slots=True)
class AggregateRegret:
    """Summary of regret across a batch of DAGs."""

    n_dags: int
    mean_regret: float
    median_regret: float
    p95_regret: float
    mean_speedup: float
    reports: tuple[RegretReport, ...] = field(default_factory=tuple)

    def positive_fraction(self) -> float:
        if not self.reports:
            return 0.0
        return sum(1 for r in self.reports if r.regret > 0) / len(self.reports)


def regret_over_dags(dags: list[DAG], num_workers: int = 2) -> AggregateRegret:
    """Compute aggregate regret across a workload of DAGs."""
    if not dags:
        return AggregateRegret(
            n_dags=0,
            mean_regret=0.0,
            median_regret=0.0,
            p95_regret=0.0,
            mean_speedup=1.0,
            reports=(),
        )
    reports = tuple(regret(d, num_workers) for d in dags)
    regrets = [r.regret for r in reports]
    speedups = [r.speedup for r in reports]
    regrets_sorted = sorted(regrets)
    p95_idx = max(0, min(len(regrets_sorted) - 1, int(0.95 * len(regrets_sorted))))
    return AggregateRegret(
        n_dags=len(reports),
        mean_regret=statistics.fmean(regrets),
        median_regret=statistics.median(regrets),
        p95_regret=regrets_sorted[p95_idx],
        mean_speedup=statistics.fmean(speedups),
        reports=reports,
    )


__all__ = ["AggregateRegret", "RegretReport", "regret", "regret_over_dags"]
