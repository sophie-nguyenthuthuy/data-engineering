"""Pointstamp accounting + invariant: counts never go negative.

A pointstamp is `(operator_id, timestamp)`. The tracker maintains a count
per pointstamp:
  - +1 when an emit produces a record at that pointstamp
  - -1 when an op processes a record at that pointstamp

When count drops to zero, the pointstamp is no longer "active" at that op.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field

from timely.timestamp.ts import Timestamp

PointStamp = tuple[str, Timestamp]


@dataclass
class TrackerStats:
    updates: int = 0
    completions: int = 0      # times count hit 0


@dataclass
class ProgressTracker:
    """Counts pending pointstamps. Thread-safe via RLock."""

    counts: dict[PointStamp, int] = field(default_factory=lambda: defaultdict(int))
    stats: TrackerStats = field(default_factory=TrackerStats)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def update(self, op: str, ts: Timestamp, delta: int) -> None:
        ps = (op, ts)
        with self._lock:
            self.counts[ps] += delta
            self.stats.updates += 1
            if self.counts[ps] == 0:
                del self.counts[ps]
                self.stats.completions += 1
            elif self.counts[ps] < 0:
                # Invariant violation: should never happen
                raise InvariantViolation(
                    f"count at {ps} went negative ({self.counts[ps]})"
                )

    def count(self, op: str, ts: Timestamp) -> int:
        with self._lock:
            return self.counts.get((op, ts), 0)

    def active_pointstamps(self) -> list[PointStamp]:
        with self._lock:
            return list(self.counts.keys())

    def total_pending(self) -> int:
        with self._lock:
            return sum(self.counts.values())

    def is_complete_at(self, op: str, t: Timestamp) -> bool:
        """No pointstamp ≤ t is active at `op`."""
        with self._lock:
            return not any(
                ts <= t for (o, ts) in self.counts if o == op
            )


class InvariantViolation(Exception):
    """Raised when a progress-tracker invariant fails."""


__all__ = ["InvariantViolation", "PointStamp", "ProgressTracker", "TrackerStats"]
