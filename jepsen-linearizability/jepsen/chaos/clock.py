"""Clock skew injection.

Each node exposes a `node_time()` method that adds an offset to wall clock.
The chaos nemesis shifts this offset to simulate NTP drift, leap seconds,
or misconfigured clocks — conditions that break "last write wins" replication.
"""

from __future__ import annotations

import random
import threading
import time
from typing import Dict

from .nemesis import Nemesis


class ClockRegistry:
    """Tracks per-node clock offsets (in seconds)."""

    def __init__(self) -> None:
        self._offsets: Dict[int, float] = {}
        self._lock = threading.Lock()

    def now(self, node_id: int) -> float:
        with self._lock:
            return time.monotonic() + self._offsets.get(node_id, 0.0)

    def skew(self, node_id: int, delta_seconds: float) -> None:
        with self._lock:
            self._offsets[node_id] = self._offsets.get(node_id, 0.0) + delta_seconds

    def reset(self, node_id: int | None = None) -> None:
        with self._lock:
            if node_id is None:
                self._offsets.clear()
            else:
                self._offsets.pop(node_id, None)

    def offsets(self) -> Dict[int, float]:
        with self._lock:
            return dict(self._offsets)


class ClockSkewNemesis(Nemesis):
    """Randomly skews the clock on a subset of nodes."""

    def __init__(
        self,
        registry: ClockRegistry,
        node_ids: list[int],
        max_skew_seconds: float = 5.0,
    ) -> None:
        self._registry = registry
        self._node_ids = node_ids
        self._max_skew = max_skew_seconds
        self._affected: list[int] = []

    def start(self) -> None:
        count = max(1, len(self._node_ids) // 2)
        self._affected = random.sample(self._node_ids, count)
        for node_id in self._affected:
            delta = random.uniform(-self._max_skew, self._max_skew)
            self._registry.skew(node_id, delta)

    def stop(self) -> None:
        for node_id in self._affected:
            self._registry.reset(node_id)
        self._affected = []

    def describe(self) -> str:
        return f"ClockSkew(±{self._max_skew}s)"
