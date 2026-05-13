"""Task DAG primitives.

A :class:`Task` is an opaque node id paired with a non-negative
duration and a list of upstream dependency ids. A :class:`DAG` is a
plain dict of `Task`s plus three operations the scheduler needs:

  * :meth:`DAG.topo_order` — Kahn's algorithm; raises
    :class:`CycleError` if the graph has a cycle or names a missing
    dependency.
  * :meth:`DAG.successors` — reverse adjacency map.
  * :meth:`DAG.critical_path_length` — earliest-finish-time DP that
    returns the makespan lower bound and per-task EFT values.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field


class CycleError(ValueError):
    """Raised when a DAG contains a cycle or references an unknown task."""


@dataclass(frozen=True, slots=True)
class Task:
    """A schedulable unit of work."""

    id: str
    duration: float
    deps: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("task id must be non-empty")
        if self.duration < 0:
            raise ValueError("task duration must be ≥ 0")
        if len(set(self.deps)) != len(self.deps):
            raise ValueError(f"duplicate dependency in task {self.id!r}")
        if self.id in self.deps:
            raise ValueError(f"task {self.id!r} cannot depend on itself")


@dataclass
class DAG:
    """Mutable task DAG indexed by task id."""

    tasks: dict[str, Task] = field(default_factory=dict)

    def add(self, task: Task) -> None:
        if task.id in self.tasks:
            raise ValueError(f"duplicate task id {task.id!r}")
        self.tasks[task.id] = task

    def __len__(self) -> int:
        return len(self.tasks)

    def __contains__(self, tid: str) -> bool:
        return tid in self.tasks

    def successors(self) -> dict[str, list[str]]:
        """Reverse adjacency: ``tid → [children]``."""
        succ: dict[str, list[str]] = defaultdict(list)
        for t in self.tasks.values():
            for d in t.deps:
                if d not in self.tasks:
                    raise CycleError(f"task {t.id!r} depends on unknown task {d!r}")
                succ[d].append(t.id)
        return dict(succ)

    def topo_order(self) -> list[str]:
        """Kahn's algorithm; raises :class:`CycleError` on cycles."""
        indeg: dict[str, int] = dict.fromkeys(self.tasks, 0)
        for t in self.tasks.values():
            for d in t.deps:
                if d not in self.tasks:
                    raise CycleError(f"task {t.id!r} depends on unknown task {d!r}")
                indeg[t.id] += 1
        succ = self.successors()
        ready = sorted([tid for tid, d in indeg.items() if d == 0])
        order: list[str] = []
        while ready:
            n = ready.pop(0)
            order.append(n)
            for s in succ.get(n, []):
                indeg[s] -= 1
                if indeg[s] == 0:
                    # Insert sorted so output is deterministic.
                    ready.append(s)
                    ready.sort()
        if len(order) != len(self.tasks):
            raise CycleError("DAG contains a cycle")
        return order

    def critical_path_length(self) -> tuple[float, dict[str, float]]:
        """Returns (CP length, EFT per task)."""
        eft: dict[str, float] = {}
        for tid in self.topo_order():
            t = self.tasks[tid]
            start = max((eft[d] for d in t.deps), default=0.0)
            eft[tid] = start + t.duration
        return (max(eft.values(), default=0.0), eft)


__all__ = ["DAG", "CycleError", "Task"]
