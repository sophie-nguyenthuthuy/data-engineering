"""Liveness watchers — must EVENTUALLY hold.

We can't prove liveness from a single state; we observe across a finite
window. `EventualDeliveryWatcher` tracks per-record latency from PG insert
to rev_etl publish. If `max_steps` elapse without delivery, we flag the
record.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tlavp.state.machine import Record, State


@dataclass
class EventualDeliveryWatcher:
    """Tracks per-record (insert_step -> delivered_step) latencies."""

    max_steps_to_delivery: int = 1000
    _insert_steps: dict[Record, int] = field(default_factory=dict)
    _delivery_steps: dict[Record, int] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def observe(self, state: State, step: int) -> list[str]:
        """Update tracking; return list of overdue records."""
        violations: list[str] = []
        with self._lock:
            # New inserts: not yet tracked
            for r in state.pg:
                if r not in self._insert_steps:
                    self._insert_steps[r] = step
            # Deliveries: just-arrived to rev_etl
            for r in state.rev_etl:
                if r not in self._delivery_steps:
                    self._delivery_steps[r] = step
            # Overdue: in pg but not delivered for > max_steps_to_delivery
            for r, ins in self._insert_steps.items():
                if r in self._delivery_steps:
                    continue
                if step - ins > self.max_steps_to_delivery:
                    violations.append(
                        f"EventualDelivery: {r} not delivered after {step - ins} steps"
                    )
        return violations

    def average_latency(self) -> float:
        with self._lock:
            deltas = [
                self._delivery_steps[r] - self._insert_steps[r]
                for r in self._delivery_steps
                if r in self._insert_steps
            ]
            return sum(deltas) / len(deltas) if deltas else 0.0


__all__ = ["EventualDeliveryWatcher"]
