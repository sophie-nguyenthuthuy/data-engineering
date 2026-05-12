"""Single-worker dataflow runtime.

Each operator has an input queue. The runtime drains queues round-robin,
updating the progress tracker on every emit and process.

For iterate-scope operators (`feedback=True`), emit() bumps the
iteration component of the timestamp on output.

Invariant we enforce: pointstamp counts never go negative (ProgressTracker
raises InvariantViolation if violated).
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from timely.progress.tracker import ProgressTracker

if TYPE_CHECKING:
    from timely.graph.builder import GraphBuilder
    from timely.graph.operator import Operator
    from timely.timestamp.ts import Timestamp


@dataclass
class RuntimeStats:
    steps: int = 0
    emits: int = 0


@dataclass
class Runtime:
    graph: GraphBuilder
    tracker: ProgressTracker = field(default_factory=ProgressTracker)
    _queues: dict[str, deque[tuple[Timestamp, Any]]] = field(
        default_factory=lambda: defaultdict(deque)
    )
    stats: RuntimeStats = field(default_factory=RuntimeStats)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self.graph.validate()
        # Seed initial inputs
        for op_name, records in self.graph.initial_inputs.items():
            for ts, value in records:
                self._enqueue(op_name, ts, value)

    def _enqueue(self, op_name: str, ts: Timestamp, value: Any) -> None:
        with self._lock:
            self._queues[op_name].append((ts, value))
            self.tracker.update(op_name, ts, +1)

    def _dequeue(self, op_name: str) -> tuple[Timestamp, Any] | None:
        with self._lock:
            if not self._queues[op_name]:
                return None
            ts, value = self._queues[op_name].popleft()
            self.tracker.update(op_name, ts, -1)
            return ts, value

    def step(self) -> bool:
        """Process one record from one non-empty queue. Returns True if work
        was done, False if all queues are empty."""
        for op_name, op in self.graph.operators.items():
            with self._lock:
                if not self._queues[op_name]:
                    continue
            entry = self._dequeue(op_name)
            if entry is None:
                continue
            ts, value = entry
            self._run_op(op, ts, value)
            return True
        return False

    def run(self, max_steps: int = 1_000_000) -> None:
        for _ in range(max_steps):
            if not self.step():
                return
        raise RuntimeError("runtime did not converge within max_steps")

    # ---- Op execution ----------------------------------------------------

    def _run_op(self, op: Operator, ts: Timestamp, value: Any) -> None:
        self.stats.steps += 1

        def emit(downstream: str, out_ts: Timestamp, out_value: Any) -> None:
            new_ts = out_ts.next_iter() if op.feedback else out_ts
            self._enqueue(downstream, new_ts, out_value)
            self.stats.emits += 1

        op.fn(ts, value, emit)

    # ---- Diagnostics -----------------------------------------------------

    def queues_empty(self) -> bool:
        with self._lock:
            return not any(self._queues[op] for op in self.graph.operators)

    def active_pointstamps(self) -> list[tuple[str, Timestamp]]:
        return self.tracker.active_pointstamps()


__all__ = ["Runtime", "RuntimeStats"]
