"""Single-worker timely dataflow graph.

Operators are pure functions of (timestamp, value) → list[(timestamp, value)].
The runtime maintains pending message queues and the progress tracker.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Callable

from .timestamp import Timestamp
from .progress import ProgressTracker


@dataclass
class Operator:
    name: str
    fn: Callable             # (ts, value, emit_fn) -> None
    inputs: list = field(default_factory=list)  # upstream op names
    feedback: bool = False   # if True, increments iteration on output


@dataclass
class Graph:
    """A simple timely-dataflow graph.

    Records flow as (op_name, ts, value). The runtime drains by location.
    """
    ops: dict = field(default_factory=dict)
    queues: dict = field(default_factory=lambda: defaultdict(deque))
    tracker: ProgressTracker = field(default_factory=ProgressTracker)
    sinks: dict = field(default_factory=lambda: defaultdict(list))

    # ---- Construction -----------------------------------------------------

    def add(self, name: str, fn: Callable, inputs: list = None, feedback: bool = False):
        self.ops[name] = Operator(name=name, fn=fn, inputs=inputs or [], feedback=feedback)

    def add_sink(self, name: str):
        """Sink ops simply accumulate emitted records."""
        def fn(ts, value, emit):
            self.sinks[name].append((ts, value))
        self.add(name, fn, inputs=[])

    # ---- Execution --------------------------------------------------------

    def send(self, location: str, ts: Timestamp, value) -> None:
        self.queues[location].append((ts, value))
        self.tracker.update(location, ts, +1)

    def emit_to(self, downstream: str, ts: Timestamp, value, feedback: bool = False) -> None:
        new_ts = ts.next_iter() if feedback else ts
        self.queues[downstream].append((new_ts, value))
        self.tracker.update(downstream, new_ts, +1)

    def run(self, max_steps: int = 10_000) -> None:
        steps = 0
        while any(self.queues[op] for op in self.ops) and steps < max_steps:
            for op_name, op in list(self.ops.items()):
                if not self.queues[op_name]:
                    continue
                ts, value = self.queues[op_name].popleft()
                self.tracker.update(op_name, ts, -1)

                def emit(downstream, out_ts, out_val, fb=op.feedback):
                    self.emit_to(downstream, out_ts, out_val, feedback=fb)
                op.fn(ts, value, emit)
                steps += 1
        if steps >= max_steps:
            raise RuntimeError("timely graph did not converge")

    def frontier(self, op_name: str) -> set[Timestamp]:
        return self.tracker.frontier(op_name)


__all__ = ["Operator", "Graph"]
