from __future__ import annotations
from ..event import Event
from .base import Watermark


class FixedLagWatermark(Watermark):
    """
    Classic fixed-lag watermark.

    watermark = max_observed_event_time - lag_seconds

    Simple and predictable but can be either too conservative (high lag wastes
    latency) or too aggressive (low lag drops valid late events).
    """

    def __init__(self, lag_seconds: float = 30.0) -> None:
        super().__init__()
        self.lag_seconds = lag_seconds
        self._max_event_time: float = float("-inf")

    def update(self, event: Event) -> float:
        if event.event_time > self._max_event_time:
            self._max_event_time = event.event_time
        self._watermark = self._max_event_time - self.lag_seconds
        return self._watermark

    def __repr__(self) -> str:
        return (
            f"FixedLagWatermark(lag={self.lag_seconds}s, "
            f"watermark={self._watermark:.3f})"
        )
