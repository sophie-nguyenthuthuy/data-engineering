"""Frontier computation.

The frontier of an operator is the antichain of *minimal active timestamps*
at that operator. When the frontier advances past timestamp t, the
operator is guaranteed never to see another record at timestamp ≤ t.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from timely.timestamp.antichain import Antichain

if TYPE_CHECKING:
    from timely.progress.tracker import ProgressTracker
    from timely.timestamp.ts import Timestamp


@dataclass
class Frontier:
    tracker: ProgressTracker
    _last_global: Antichain = field(default_factory=Antichain)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def at_operator(self, op: str) -> Antichain:
        """Antichain of minimal active timestamps at operator `op`."""
        active = [ts for (o, ts) in self.tracker.active_pointstamps() if o == op]
        chain = Antichain()
        for t in active:
            # Only keep if no other active element strictly dominates t
            if not any(s < t for s in active):
                chain.insert(t)
        return chain

    def global_frontier(self) -> Antichain:
        """Antichain of timestamps not yet complete anywhere in the graph."""
        active = [ts for _op, ts in self.tracker.active_pointstamps()]
        chain = Antichain()
        for t in active:
            if not any(s < t for s in active):
                chain.insert(t)
        with self._lock:
            self._last_global = chain.copy()
        return chain

    def passed(self, t: Timestamp) -> bool:
        """True if the global frontier has passed timestamp `t` — i.e. no
        active timestamp is ≤ t."""
        return not any(ts <= t for _o, ts in self.tracker.active_pointstamps())

    @property
    def last_global(self) -> Antichain:
        with self._lock:
            return self._last_global.copy()


__all__ = ["Frontier"]
