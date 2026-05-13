"""Baseline schedulers (FCFS) used as shadow-mode counterfactuals.

The simplest reasonable scheduler: dispatch tasks in topological order,
place each one on the currently least-loaded worker after its
dependencies have completed. This is the behaviour the shadow harness
compares against to compute regret.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fps.scheduler.common import Schedule, ScheduledTask

if TYPE_CHECKING:
    from fps.dag import DAG


def baseline_fcfs_schedule(dag: DAG, num_workers: int) -> Schedule:
    """Topological-order FCFS with least-loaded-worker placement."""
    if num_workers < 1:
        raise ValueError("num_workers must be ≥ 1")
    if not dag.tasks:
        return {}

    schedule: Schedule = {}
    worker_free = [0.0] * num_workers
    for tid in dag.topo_order():
        t = dag.tasks[tid]
        dep_finish = max((schedule[d].finish for d in t.deps), default=0.0)
        best_w = min(range(num_workers), key=lambda w: (worker_free[w], w))
        start = max(worker_free[best_w], dep_finish)
        finish = start + t.duration
        schedule[tid] = ScheduledTask(start=start, finish=finish, worker=best_w)
        worker_free[best_w] = finish
    return schedule


__all__ = ["baseline_fcfs_schedule"]
