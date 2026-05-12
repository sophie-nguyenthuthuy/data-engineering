"""List scheduler (critical-path-first) + tiny branch-and-bound exact solver."""
from __future__ import annotations

import heapq
from dataclasses import dataclass, field

from .dag import DAG, Task


def list_schedule(dag: DAG, num_workers: int) -> dict:
    """Critical-path-first list scheduling.

    Returns dict {task_id -> (start, finish, worker)}.
    """
    cp, eft = dag.critical_path_length()
    # Priority = -eft (highest CP first)
    # Track when each task can start (after deps done)
    succ = dag.successors()
    indeg = {tid: len(t.deps) for tid, t in dag.tasks.items()}
    ready = [(-eft[tid], tid) for tid in dag.tasks if indeg[tid] == 0]
    heapq.heapify(ready)

    schedule = {}
    worker_free = [0.0] * num_workers       # time each worker becomes free

    while ready:
        _, tid = heapq.heappop(ready)
        t = dag.tasks[tid]
        # Earliest start = max(dep finishes); fit on earliest-free worker after that.
        dep_finish = max((schedule[d][1] for d in t.deps), default=0.0)
        # Pick the worker that becomes free at or before dep_finish and has earliest start
        best_w = min(range(num_workers), key=lambda w: max(worker_free[w], dep_finish))
        start = max(worker_free[best_w], dep_finish)
        finish = start + t.duration
        schedule[tid] = (start, finish, best_w)
        worker_free[best_w] = finish

        for s in succ.get(tid, []):
            indeg[s] -= 1
            if indeg[s] == 0:
                heapq.heappush(ready, (-eft[s], s))

    return schedule


def makespan(schedule: dict) -> float:
    if not schedule:
        return 0.0
    return max(f for _, f, _ in schedule.values())


def branch_and_bound(dag: DAG, num_workers: int, time_limit_ms: int = 100) -> dict:
    """Tiny B&B: enumerate dispatch orders for small DAGs (≤ 10 nodes).

    For larger DAGs, fall back to list_schedule.
    """
    if len(dag.tasks) > 10:
        return list_schedule(dag, num_workers)

    import time
    deadline = time.perf_counter() + time_limit_ms / 1000.0

    best = {"makespan": float("inf"), "schedule": None}

    def recurse(scheduled, worker_free):
        if time.perf_counter() > deadline:
            return
        if len(scheduled) == len(dag.tasks):
            ms = max(f for (_, f, _) in scheduled.values())
            if ms < best["makespan"]:
                best["makespan"] = ms
                best["schedule"] = dict(scheduled)
            return
        # Find ready tasks
        ready = [tid for tid, t in dag.tasks.items()
                 if tid not in scheduled
                 and all(d in scheduled for d in t.deps)]
        for tid in ready:
            t = dag.tasks[tid]
            dep_finish = max((scheduled[d][1] for d in t.deps), default=0.0)
            for w in range(num_workers):
                start = max(worker_free[w], dep_finish)
                finish = start + t.duration
                # Prune: skip if already worse than best
                if finish >= best["makespan"]:
                    continue
                scheduled[tid] = (start, finish, w)
                old = worker_free[w]
                worker_free[w] = finish
                recurse(scheduled, worker_free)
                worker_free[w] = old
                del scheduled[tid]

    recurse({}, [0.0] * num_workers)
    return best["schedule"] or list_schedule(dag, num_workers)


__all__ = ["list_schedule", "makespan", "branch_and_bound"]
