"""Progress tracking: pointstamp accounting + frontier computation.

A pointstamp is a (location, timestamp) pair where `location` is an operator
input/output port. We maintain a *count* per pointstamp:
  - +1 when a record at that timestamp is sent to that location
  - −1 when the record is processed

The frontier is the set of minimal active pointstamps. When the count of every
pointstamp ≤ t reaches zero AND no upstream op can produce ≤ t, the frontier
advances past t.

This single-process implementation maintains the invariant globally.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from .timestamp import Timestamp


PointStamp = tuple  # (location, Timestamp)


@dataclass
class ProgressTracker:
    """Counts pending pointstamps and computes frontiers."""
    counts: dict = field(default_factory=lambda: defaultdict(int))

    def update(self, location: str, ts: Timestamp, delta: int) -> None:
        ps = (location, ts)
        self.counts[ps] += delta
        if self.counts[ps] == 0:
            del self.counts[ps]

    def active_locations(self) -> set[str]:
        return {loc for (loc, _ts) in self.counts.keys()}

    def frontier(self, location: str) -> set[Timestamp]:
        """Minimal timestamps active at this location (antichain)."""
        active = [ts for (loc, ts) in self.counts.keys() if loc == location]
        if not active:
            return set()
        # Minimal elements of the partial order
        minimals: set[Timestamp] = set()
        for t in active:
            if not any(s < t for s in active):
                minimals.add(t)
        return minimals

    def is_complete_at(self, location: str, t: Timestamp) -> bool:
        """True if no pointstamp ≤ t is active at `location`."""
        for (loc, ts) in self.counts:
            if loc == location and ts <= t:
                return False
        return True

    def __repr__(self) -> str:
        return f"ProgressTracker({dict(self.counts)})"


__all__ = ["ProgressTracker", "PointStamp"]
