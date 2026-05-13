"""Shared schedule types + invariants.

A :class:`Schedule` is a mapping ``task_id → ScheduledTask`` where the
:class:`ScheduledTask` carries start / finish / assigned worker. The
:func:`assert_valid_schedule` helper enforces three invariants that
every scheduler in this package must respect:

  * **Completeness** — every task in the DAG is assigned.
  * **Dependency order** — each task starts ≥ the latest finish of its
    upstreams.
  * **Single-assignment per worker** — no two tasks overlap on the
    same worker.

Failing any invariant raises :class:`ScheduleInvariantError`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from fps.dag import DAG


class ScheduleInvariantError(AssertionError):
    """A schedule violates dependency-order or worker-exclusivity."""


@dataclass(frozen=True, slots=True)
class ScheduledTask:
    """The (start, finish, worker) triple for a single task."""

    start: float
    finish: float
    worker: int

    def __post_init__(self) -> None:
        if self.start < 0:
            raise ValueError("start must be ≥ 0")
        if self.finish < self.start:
            raise ValueError("finish must be ≥ start")
        if self.worker < 0:
            raise ValueError("worker must be ≥ 0")


Schedule: TypeAlias = dict[str, ScheduledTask]


def makespan(schedule: Schedule) -> float:
    """Latest finish time across all assigned tasks."""
    if not schedule:
        return 0.0
    return max(st.finish for st in schedule.values())


def assert_valid_schedule(
    dag: DAG, schedule: Schedule, num_workers: int, *, tol: float = 1e-9
) -> None:
    """Raise :class:`ScheduleInvariantError` if any invariant fails."""
    if num_workers < 1:
        raise ValueError("num_workers must be ≥ 1")

    missing = set(dag.tasks) - set(schedule)
    extra = set(schedule) - set(dag.tasks)
    if missing or extra:
        raise ScheduleInvariantError(
            f"schedule incomplete (missing={sorted(missing)}, extra={sorted(extra)})"
        )

    # Dependency order
    for tid, st in schedule.items():
        for dep in dag.tasks[tid].deps:
            dep_st = schedule[dep]
            if st.start + tol < dep_st.finish:
                raise ScheduleInvariantError(
                    f"task {tid!r} starts at {st.start} before dep {dep!r} finishes at {dep_st.finish}"
                )

    # Single assignment per worker
    by_worker: dict[int, list[tuple[float, float, str]]] = {w: [] for w in range(num_workers)}
    for tid, st in schedule.items():
        if st.worker >= num_workers:
            raise ScheduleInvariantError(
                f"task {tid!r} assigned to worker {st.worker} ≥ num_workers {num_workers}"
            )
        by_worker[st.worker].append((st.start, st.finish, tid))
    for w, intervals in by_worker.items():
        intervals.sort()
        for i in range(1, len(intervals)):
            prev = intervals[i - 1]
            cur = intervals[i]
            if cur[0] + tol < prev[1]:
                raise ScheduleInvariantError(
                    f"worker {w} runs {prev[2]!r} ({prev[0]}, {prev[1]}) "
                    f"and {cur[2]!r} ({cur[0]}, {cur[1]}) concurrently"
                )


__all__ = [
    "Schedule",
    "ScheduleInvariantError",
    "ScheduledTask",
    "assert_valid_schedule",
    "makespan",
]
