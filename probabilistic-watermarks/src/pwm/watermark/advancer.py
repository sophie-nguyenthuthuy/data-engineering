"""Watermark advancer with formal monotonicity guarantee.

W(t) = min over active keys k (rate ≥ λ_min) of (t - safe_delay(k))

Because safe_delay is monotone non-decreasing in calendar time AND t is
monotone non-decreasing (we only see arrival_time), the resulting W could
in principle decrease if a slow key suddenly becomes active. We enforce
monotonicity by tracking a running max.

A record with event_time < W is `late` and routed to the correction stream.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from pwm.watermark.estimator import PerKeyDelayEstimator


@dataclass
class WatermarkStats:
    on_time: int = 0
    late: int = 0
    advances: int = 0
    last_advance_amount: float = 0.0

    @property
    def late_rate(self) -> float:
        total = self.on_time + self.late
        return self.late / total if total else 0.0


@dataclass
class WatermarkAdvancer:
    """Streaming watermark with monotone non-decreasing invariant."""

    delay_estimator: PerKeyDelayEstimator
    lambda_min: float = 0.0                # ignore keys with rate < λ_min
    _w: float = 0.0
    _late_handler: Callable[[object, float, float], None] | None = None
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]
    stats: WatermarkStats = field(default_factory=WatermarkStats)

    def on_record(self, key: object, event_time: float, arrival_time: float) -> tuple[str, float]:
        """Process one record. Returns (status, watermark) where status is
        either 'ontime' or 'late'."""
        self.delay_estimator.observe(key, event_time, arrival_time)
        with self._lock:
            new_w = self._compute_locked(arrival_time)
            if new_w > self._w:
                self.stats.last_advance_amount = new_w - self._w
                self._w = new_w
                self.stats.advances += 1
            w = self._w
            late = event_time < w
            if late:
                self.stats.late += 1
            else:
                self.stats.on_time += 1
        if late and self._late_handler is not None:
            self._late_handler(key, event_time, arrival_time)
        return ("late", w) if late else ("ontime", w)

    def _compute_locked(self, now: float) -> float:
        candidates: list[float] = []
        for k in self.delay_estimator.keys():  # noqa: SIM118
            if self.delay_estimator.rate(k) < self.lambda_min:
                continue
            candidates.append(now - self.delay_estimator.safe_delay(k))
        if not candidates:
            return self._w
        return min(candidates)

    @property
    def value(self) -> float:
        with self._lock:
            return self._w

    def set_late_handler(self, fn: Callable[[object, float, float], None]) -> None:
        with self._lock:
            self._late_handler = fn
