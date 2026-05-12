"""Multi-worker progress coordinator.

In a distributed timely-dataflow system, each worker maintains a local
pointstamp count and broadcasts *deltas* to a central coordinator. The
coordinator maintains the global accountancy and recomputes the global
frontier when needed.

We implement a single-process version: workers share state via the
ProgressCoordinator object. In a real network deployment this would be
replaced by an RPC layer.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from timely.progress.tracker import ProgressTracker
from timely.timestamp.antichain import Antichain

if TYPE_CHECKING:
    from collections.abc import Callable

    from timely.timestamp.ts import Timestamp


@dataclass
class ProgressCoordinator:
    """Aggregates per-worker updates; broadcasts frontier advances."""

    tracker: ProgressTracker = field(default_factory=ProgressTracker)
    _frontier_listeners: list[Callable[[Antichain], None]] = field(default_factory=list)
    _per_worker_pending: dict[int, dict[tuple[str, Timestamp], int]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]
    _last_frontier: Antichain = field(default_factory=Antichain)
    advances: int = 0

    def worker_update(self, worker_id: int, op: str, ts: Timestamp, delta: int) -> None:
        """Apply a worker's delta. Bookkeeping is local-then-broadcast."""
        with self._lock:
            key = (op, ts)
            pending = self._per_worker_pending[worker_id]
            pending[key] = pending.get(key, 0) + delta
            self.tracker.update(op, ts, delta)
            self._maybe_advance_locked()

    def _maybe_advance_locked(self) -> None:
        # Compute current global frontier
        active = self.tracker.active_pointstamps()
        chain = Antichain()
        for _op, t in active:
            if not any(s < t for (_oo, s) in active):
                chain.insert(t)
        # Detect advance: did the new frontier strictly dominate the old?
        # We define advance as: old ≠ new and every new element is ≥ some old.
        old_els = self._last_frontier.elements()
        new_els = chain.elements()
        if old_els != new_els:
            self.advances += 1
            self._last_frontier = chain.copy()
            for fn in self._frontier_listeners:
                fn(chain)

    def subscribe(self, fn: Callable[[Antichain], None]) -> None:
        with self._lock:
            self._frontier_listeners.append(fn)

    @property
    def frontier(self) -> Antichain:
        with self._lock:
            return self._last_frontier.copy()


__all__ = ["ProgressCoordinator"]
