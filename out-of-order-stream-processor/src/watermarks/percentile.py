from __future__ import annotations
from collections import deque

from ..event import Event
from .base import Watermark


class PercentileWatermark(Watermark):
    """
    Global percentile-based watermark (no per-key differentiation).

    Maintains a sliding window of observed ingestion latencies across *all*
    keys and sets the watermark lag to the P-th percentile.  Simpler than
    DynamicPerKeyWatermark but still adaptive to changing stream latency.

    Useful as a baseline comparison against the per-key dynamic strategy.
    """

    def __init__(
        self,
        percentile: float = 90.0,
        window_size: int = 200,
        min_lag: float = 0.5,
        max_lag: float = 1800.0,
    ) -> None:
        super().__init__()
        self.percentile = percentile
        self.window_size = window_size
        self.min_lag = min_lag
        self.max_lag = max_lag

        self._latencies: deque[float] = deque(maxlen=window_size)
        self._max_event_time: float = float("-inf")

    def update(self, event: Event) -> float:
        latency = max(0.0, event.processing_time - event.event_time)
        self._latencies.append(latency)
        if event.event_time > self._max_event_time:
            self._max_event_time = event.event_time

        lag = min(max(self._current_lag(), self.min_lag), self.max_lag)
        self._watermark = self._max_event_time - lag
        return self._watermark

    def _current_lag(self) -> float:
        if not self._latencies:
            return self.min_lag
        sorted_lats = sorted(self._latencies)
        idx = (self.percentile / 100.0) * (len(sorted_lats) - 1)
        lo = int(idx)
        hi = min(lo + 1, len(sorted_lats) - 1)
        frac = idx - lo
        return sorted_lats[lo] * (1 - frac) + sorted_lats[hi] * frac

    @property
    def current_lag(self) -> float:
        return self._current_lag()

    def __repr__(self) -> str:
        return (
            f"PercentileWatermark(p={self.percentile}, "
            f"lag={self._current_lag():.3f}s, "
            f"watermark={self._watermark:.3f})"
        )
