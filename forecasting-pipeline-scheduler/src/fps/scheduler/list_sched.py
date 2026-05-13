"""Critical-path-first list scheduling.

A classic list scheduler:

  1. Compute each task's bottom-level (``b_level``) — the longest
     directed path from the task to any sink, weighted by duration.
     Tasks on the critical path have the highest ``b_level``.
  2. Maintain a min-heap of ready tasks keyed by ``(-b_level, id)``.
  3. Pop a task, place it on the worker whose earliest-feasible-start
     is smallest, advance that worker's clock.

Output is a :class:`Schedule` mapping each task id to a
:class:`ScheduledTask`.
"""

from __future__ import annotations

import heapq
from typing import TYPE_CHECKING

from fps.scheduler.common import Schedule, ScheduledTask

if TYPE_CHECKING:
    from fps.dag import DAG


def _b_levels(dag: DAG) -> dict[str, float]:
    """Longest path *down* to a sink (inclusive of task's own duration)."""
    order = dag.topo_order()
    succ = dag.successors()
    bl: dict[str, float] = {}
    for tid in reversed(order):
        downstream = max((bl[s] for s in succ.get(tid, [])), default=0.0)
        bl[tid] = dag.tasks[tid].duration + downstream
    return bl


def list_schedule(dag: DAG, num_workers: int) -> Schedule:
    """Critical-path-first list scheduler with worker-best-fit placement."""
    if num_workers < 1:
        raise ValueError("num_workers must be ≥ 1")
    if not dag.tasks:
        return {}

    bl = _b_levels(dag)
    succ = dag.successors()
    indeg: dict[str, int] = {tid: len(t.deps) for tid, t in dag.tasks.items()}

    ready: list[tuple[float, str]] = [(-bl[tid], tid) for tid, d in indeg.items() if d == 0]
    heapq.heapify(ready)

    schedule: Schedule = {}
    worker_free = [0.0] * num_workers

    while ready:
        _, tid = heapq.heappop(ready)
        task = dag.tasks[tid]
        dep_finish = max((schedule[d].finish for d in task.deps), default=0.0)
        # Pick the worker whose earliest feasible start is smallest;
        # break ties by index for deterministic output.
        best_w = min(
            range(num_workers),
            key=lambda w: (max(worker_free[w], dep_finish), w),
        )
        start = max(worker_free[best_w], dep_finish)
        finish = start + task.duration
        schedule[tid] = ScheduledTask(start=start, finish=finish, worker=best_w)
        worker_free[best_w] = finish
        for s in succ.get(tid, []):
            indeg[s] -= 1
            if indeg[s] == 0:
                heapq.heappush(ready, (-bl[s], s))

    return schedule


__all__ = ["list_schedule"]
