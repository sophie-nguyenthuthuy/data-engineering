"""Shadow-mode regret measurement.

Given two schedulers, run them both on the same DAGs (in simulation),
measure makespan, report regret = baseline - ours.
"""
from __future__ import annotations

from dataclasses import dataclass

from .dag import DAG
from .scheduler import list_schedule, makespan


@dataclass
class RegretReport:
    baseline_makespan: float
    our_makespan: float
    regret: float

    @property
    def speedup(self) -> float:
        if self.our_makespan == 0:
            return 1.0
        return self.baseline_makespan / self.our_makespan


def baseline_fcfs_schedule(dag: DAG, num_workers: int) -> dict:
    """Naive FCFS: schedule in topological order with no CP priority."""
    sched = {}
    worker_free = [0.0] * num_workers
    for tid in dag.topo_order():
        t = dag.tasks[tid]
        dep_finish = max((sched[d][1] for d in t.deps), default=0.0)
        best_w = min(range(num_workers), key=lambda w: worker_free[w])
        start = max(worker_free[best_w], dep_finish)
        finish = start + t.duration
        sched[tid] = (start, finish, best_w)
        worker_free[best_w] = finish
    return sched


def regret(dag: DAG, num_workers: int = 2) -> RegretReport:
    baseline = baseline_fcfs_schedule(dag, num_workers)
    ours = list_schedule(dag, num_workers)
    return RegretReport(
        baseline_makespan=makespan(baseline),
        our_makespan=makespan(ours),
        regret=makespan(baseline) - makespan(ours),
    )


__all__ = ["RegretReport", "baseline_fcfs_schedule", "regret"]
