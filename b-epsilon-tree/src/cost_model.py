"""Online ε tuner.

Watches read/write ratio in a sliding window and recommends an ε that
minimises expected cost. Hysteresis prevents thrashing.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass


@dataclass
class WorkloadStats:
    reads: int = 0
    writes: int = 0

    @property
    def total(self) -> int:
        return self.reads + self.writes

    @property
    def write_frac(self) -> float:
        return self.writes / self.total if self.total else 0.0


class EpsilonTuner:
    def __init__(self, window: int = 1000, hysteresis: float = 0.05):
        self.window = window
        self.hysteresis = hysteresis
        self._events: deque = deque(maxlen=window)
        self._current_eps = 0.5

    def observe(self, op: str) -> None:
        """op in {'read','write'}"""
        self._events.append(op)

    def stats(self) -> WorkloadStats:
        r = sum(1 for e in self._events if e == "read")
        w = sum(1 for e in self._events if e == "write")
        return WorkloadStats(reads=r, writes=w)

    def recommend(self, B: int = 16) -> float:
        """Heuristic: ε ≈ write_frac (capped). Specifically:
            - all-reads  → ε ~ 0.1  (most space for pivots = B+-tree-like)
            - all-writes → ε ~ 0.9  (most space for buffer)
        Smooth with hysteresis.
        """
        s = self.stats()
        if s.total == 0:
            return self._current_eps
        target = 0.1 + 0.8 * s.write_frac
        # Hysteresis
        if abs(target - self._current_eps) > self.hysteresis:
            self._current_eps = target
        return self._current_eps

    @property
    def current(self) -> float:
        return self._current_eps


__all__ = ["WorkloadStats", "EpsilonTuner"]
