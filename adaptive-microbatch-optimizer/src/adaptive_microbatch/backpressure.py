"""
Downstream backpressure monitor.

Downstream workers signal pressure by calling push() with a load factor
in [0, 1]. The monitor aggregates these signals into a single pressure
level that the window manager uses to slow intake.
"""

import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Deque, Optional


@dataclass
class PressureSignal:
    timestamp: float
    source: str
    level: float  # 0.0 = no pressure … 1.0 = fully saturated


class BackpressureMonitor:
    """
    Collects pressure signals from N downstream workers and exposes a
    weighted-average pressure level over a rolling time window.
    """

    def __init__(self, window_seconds: float = 5.0) -> None:
        self.window_seconds = window_seconds
        self._signals: Deque[PressureSignal] = deque()
        self._on_pressure_change: Optional[Callable[[float], None]] = None

    def on_pressure_change(self, callback: Callable[[float], None]) -> None:
        """Register a callback invoked whenever the aggregated level changes."""
        self._on_pressure_change = callback

    def push(self, source: str, level: float) -> None:
        """
        Called by a downstream worker to report its current load factor.

        Args:
            source: Worker identifier (e.g. "db-sink-1").
            level:  Load factor in [0.0, 1.0]. Values outside are clamped.
        """
        level = max(0.0, min(1.0, level))
        self._signals.append(
            PressureSignal(timestamp=time.monotonic(), source=source, level=level)
        )
        self._evict()
        if self._on_pressure_change:
            self._on_pressure_change(self.current_level())

    def _evict(self) -> None:
        cutoff = time.monotonic() - self.window_seconds
        while self._signals and self._signals[0].timestamp < cutoff:
            self._signals.popleft()

    def current_level(self) -> float:
        """
        Return the exponentially-weighted average pressure, biased toward
        recent signals.  Returns 0.0 if no signals in the window.
        """
        self._evict()
        if not self._signals:
            return 0.0

        now = time.monotonic()
        weight_sum = 0.0
        value_sum = 0.0
        for sig in self._signals:
            age = now - sig.timestamp
            # Exponential decay: half-life = window_seconds / 3
            w = 2 ** (-age / (self.window_seconds / 3))
            weight_sum += w
            value_sum += w * sig.level

        return value_sum / weight_sum if weight_sum > 0 else 0.0

    def is_saturated(self, threshold: float = 0.85) -> bool:
        return self.current_level() >= threshold

    def clear(self) -> None:
        self._signals.clear()
