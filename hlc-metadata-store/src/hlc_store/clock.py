from __future__ import annotations

import threading
import time
from collections.abc import Callable

from .timestamp import ZERO, HLCTimestamp


class HybridLogicalClock:
    """
    Hybrid Logical Clock as described in Kulkarni et al., "Logical Physical Clocks
    and Consistent Snapshots in Globally Distributed Databases" (2014).

    Guarantees:
      - ts(e) >= wall_clock(e) for every event e.
      - e → f  ⟹  ts(e) < ts(f)  (causal order always reflected in timestamps).
      - The logical component never exceeds O(n) where n is the number of concurrent
        events — it resets to 0 whenever wall time advances.
    """

    def __init__(
        self,
        node_id: str,
        wall_fn: Callable[[], int] | None = None,
        drift_ms: int = 0,
    ) -> None:
        self.node_id = node_id
        self._drift_ms = drift_ms
        self._wall_fn: Callable[[], int] = wall_fn or (lambda: int(time.time() * 1000))
        self._ts = ZERO
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Clock drift simulation
    # ------------------------------------------------------------------

    @property
    def drift_ms(self) -> int:
        return self._drift_ms

    @drift_ms.setter
    def drift_ms(self, value: int) -> None:
        with self._lock:
            self._drift_ms = value

    def _wall(self) -> int:
        return self._wall_fn() + self._drift_ms

    # ------------------------------------------------------------------
    # Core HLC operations
    # ------------------------------------------------------------------

    def tick(self) -> HLCTimestamp:
        """Advance clock for a local event or message send."""
        with self._lock:
            wall = self._wall()
            if wall > self._ts.wall_ms:
                self._ts = HLCTimestamp(wall, 0)
            else:
                self._ts = HLCTimestamp(self._ts.wall_ms, self._ts.logical + 1)
            return self._ts

    def update(self, remote: HLCTimestamp) -> HLCTimestamp:
        """Advance clock upon receiving a message carrying timestamp *remote*."""
        with self._lock:
            wall = self._wall()
            l_prime = max(wall, remote.wall_ms, self._ts.wall_ms)

            if l_prime == self._ts.wall_ms == remote.wall_ms:
                logical = max(self._ts.logical, remote.logical) + 1
            elif l_prime == self._ts.wall_ms:
                logical = self._ts.logical + 1
            elif l_prime == remote.wall_ms:
                logical = remote.logical + 1
            else:
                logical = 0

            self._ts = HLCTimestamp(l_prime, logical)
            return self._ts

    def peek(self) -> HLCTimestamp:
        """Return current timestamp without advancing."""
        with self._lock:
            return self._ts


class WallClock:
    """Baseline: pure wall-clock timestamp (no causal guarantees)."""

    def __init__(
        self,
        node_id: str,
        drift_ms: int = 0,
        wall_fn: Callable[[], int] | None = None,
    ) -> None:
        self.node_id = node_id
        self._drift_ms = drift_ms
        self._wall_fn: Callable[[], int] = wall_fn or (lambda: int(time.time() * 1000))
        self._lock = threading.Lock()

    @property
    def drift_ms(self) -> int:
        return self._drift_ms

    @drift_ms.setter
    def drift_ms(self, value: int) -> None:
        with self._lock:
            self._drift_ms = value

    def _wall(self) -> int:
        return self._wall_fn() + self._drift_ms

    def tick(self) -> HLCTimestamp:
        return HLCTimestamp(self._wall(), 0)

    def update(self, remote: HLCTimestamp) -> HLCTimestamp:
        # Wall-clock systems ignore remote timestamps — this is the root of all evil.
        return HLCTimestamp(self._wall(), 0)

    def peek(self) -> HLCTimestamp:
        return HLCTimestamp(self._wall(), 0)
