"""Watermark advancer with formal monotonicity.

The watermark W(t) is the maximum event-time t* such that for all active keys
k with rate > λ_min, t - safe_delay(k) ≥ t*. Because safe_delay is monotone
non-decreasing in time, W is monotone non-decreasing too.

For records arriving after their key's watermark, route to a CORRECTION stream
that downstream consumers idempotently apply.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from .delay_estimator import PerKeyDelayEstimator


@dataclass
class WatermarkAdvancer:
    delay_estimator: PerKeyDelayEstimator
    lambda_min: float = 0.01             # ignore keys with rate < this
    _w: float = 0.0
    _late_handler: Callable | None = None

    def on_record(self, key, event_time: float, arrival_time: float):
        """Process one record. Either it's on-time → forward, or late → correction."""
        self.delay_estimator.observe(key, event_time, arrival_time)
        # Compute new watermark from current state (conservative)
        new_w = self._compute_watermark(arrival_time)
        # Monotone guard
        if new_w > self._w:
            self._w = new_w
        if event_time < self._w:
            if self._late_handler is not None:
                self._late_handler(key, event_time, arrival_time)
            return "late", self._w
        return "ontime", self._w

    def _compute_watermark(self, now: float) -> float:
        """W(t) = min over active keys (rate > λ_min) of (t - q_k(1-δ))."""
        candidates = []
        for k in self.delay_estimator.keys():
            if self.delay_estimator.rate(k) < self.lambda_min:
                continue
            candidates.append(now - self.delay_estimator.safe_delay(k))
        if not candidates:
            return self._w  # no advance
        return min(candidates)

    @property
    def value(self) -> float:
        return self._w

    def set_late_handler(self, fn: Callable):
        self._late_handler = fn


__all__ = ["WatermarkAdvancer"]
