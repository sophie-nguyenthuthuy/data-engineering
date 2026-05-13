"""Branch-and-bound scheduler for small DAGs.

Exhaustively enumerates feasible dispatch decisions (which ready task
to place on which worker next), pruning a partial schedule when its
lower bound — the critical-path length remaining + the current best
finish — is already ≥ the incumbent makespan.

For DAGs larger than ``max_tasks`` (default 12) or when the time limit
expires the function falls back to :func:`list_schedule` so the caller
is guaranteed a valid schedule on every input.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from fps.scheduler.common import Schedule, ScheduledTask
from fps.scheduler.list_sched import list_schedule

if TYPE_CHECKING:
    from fps.dag import DAG


def _b_levels(dag: DAG) -> dict[str, float]:
    """Longest path down to a sink, inclusive of task's own duration."""
    order = dag.topo_order()
    succ = dag.successors()
    bl: dict[str, float] = {}
    for tid in reversed(order):
        bl[tid] = dag.tasks[tid].duration + max((bl[s] for s in succ.get(tid, [])), default=0.0)
    return bl


def branch_and_bound(
    dag: DAG,
    num_workers: int,
    *,
    time_limit_ms: int = 100,
    max_tasks: int = 12,
) -> Schedule:
    """Exact small-DAG scheduler with greedy fallback."""
    if num_workers < 1:
        raise ValueError("num_workers must be ≥ 1")
    if time_limit_ms <= 0:
        raise ValueError("time_limit_ms must be > 0")
    if not dag.tasks:
        return {}
    if len(dag.tasks) > max_tasks:
        return list_schedule(dag, num_workers)

    # Incumbent: the list-scheduler result. Any feasible schedule is a
    # valid upper bound; B&B only accepts strict improvements.
    incumbent = list_schedule(dag, num_workers)
    best_makespan = max((st.finish for st in incumbent.values()), default=0.0)
    best: Schedule = dict(incumbent)

    deadline = time.perf_counter() + time_limit_ms / 1000.0
    bl = _b_levels(dag)
    succ = dag.successors()
    indeg0: dict[str, int] = {tid: len(t.deps) for tid, t in dag.tasks.items()}

    def recurse(
        scheduled: Schedule,
        worker_free: list[float],
        indeg: dict[str, int],
        cur_finish: float,
    ) -> None:
        nonlocal best_makespan, best
        if time.perf_counter() > deadline:
            return
        if len(scheduled) == len(dag.tasks):
            ms = max(st.finish for st in scheduled.values())
            if ms < best_makespan - 1e-12:
                best_makespan = ms
                best = dict(scheduled)
            return
        # Lower bound: critical-path of any remaining ready node + its
        # b_level can't finish before cur_finish.
        remaining = [tid for tid in dag.tasks if tid not in scheduled]
        lb = max(cur_finish, max((bl[tid] for tid in remaining), default=0.0))
        if lb >= best_makespan - 1e-12:
            return
        ready = sorted(tid for tid in remaining if indeg[tid] == 0 and tid not in scheduled)
        for tid in ready:
            task = dag.tasks[tid]
            dep_finish = max((scheduled[d].finish for d in task.deps), default=0.0)
            for w in range(num_workers):
                start = max(worker_free[w], dep_finish)
                finish = start + task.duration
                if finish >= best_makespan - 1e-12:
                    continue
                old = worker_free[w]
                worker_free[w] = finish
                scheduled[tid] = ScheduledTask(start=start, finish=finish, worker=w)
                # Decrement indegrees of successors for the next recursion.
                released: list[str] = []
                for s in succ.get(tid, []):
                    indeg[s] -= 1
                    released.append(s)
                recurse(scheduled, worker_free, indeg, max(cur_finish, finish))
                for s in released:
                    indeg[s] += 1
                worker_free[w] = old
                del scheduled[tid]

    recurse({}, [0.0] * num_workers, dict(indeg0), 0.0)
    return best


__all__ = ["branch_and_bound"]
