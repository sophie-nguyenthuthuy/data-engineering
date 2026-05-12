"""Task DAG primitives + critical-path computation."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class Task:
    id: str
    duration: float
    deps: list = field(default_factory=list)


@dataclass
class DAG:
    tasks: dict = field(default_factory=dict)        # id -> Task

    def add(self, task: Task) -> None:
        self.tasks[task.id] = task

    def successors(self) -> dict:
        succ = defaultdict(list)
        for t in self.tasks.values():
            for d in t.deps:
                succ[d].append(t.id)
        return succ

    def topo_order(self) -> list:
        indeg = defaultdict(int)
        for t in self.tasks.values():
            for d in t.deps:
                indeg[t.id] += 1
        ready = [tid for tid in self.tasks if indeg[tid] == 0]
        order = []
        succ = self.successors()
        while ready:
            n = ready.pop()
            order.append(n)
            for s in succ.get(n, []):
                indeg[s] -= 1
                if indeg[s] == 0:
                    ready.append(s)
        return order

    def critical_path_length(self) -> tuple[float, dict]:
        """Earliest finish time per task (using forecasted durations); CP length = max."""
        eft = {}
        for tid in self.topo_order():
            t = self.tasks[tid]
            start = max((eft[d] for d in t.deps), default=0.0)
            eft[tid] = start + t.duration
        return max(eft.values(), default=0.0), eft


__all__ = ["Task", "DAG"]
