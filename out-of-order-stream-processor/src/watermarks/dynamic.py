from __future__ import annotations
from collections import defaultdict, deque
from typing import Optional
import math

from ..event import Event
from .base import Watermark


class _KeyStats:
    """Running latency statistics for a single key using a sliding window."""

    def __init__(self, window_size: int, percentile: float) -> None:
        self.window_size = window_size
        self.percentile = percentile
        self._latencies: deque[float] = deque(maxlen=window_size)
        self._max_event_time: float = float("-inf")

    def observe(self, event: Event) -> None:
        latency = max(0.0, event.processing_time - event.event_time)
        self._latencies.append(latency)
        if event.event_time > self._max_event_time:
            self._max_event_time = event.event_time

    @property
    def lag(self) -> float:
        """Current lag estimate at the configured percentile."""
        if not self._latencies:
            return 0.0
        sorted_lats = sorted(self._latencies)
        idx = (self.percentile / 100.0) * (len(sorted_lats) - 1)
        lo = int(idx)
        hi = min(lo + 1, len(sorted_lats) - 1)
        frac = idx - lo
        return sorted_lats[lo] * (1 - frac) + sorted_lats[hi] * frac

    @property
    def max_event_time(self) -> float:
        return self._max_event_time

    @property
    def sample_count(self) -> int:
        return len(self._latencies)


class DynamicPerKeyWatermark(Watermark):
    """
    Adaptive, per-key watermark based on observed ingestion-latency distribution.

    For each key, we track a sliding window of (processing_time - event_time)
    samples and derive the watermark lag as the P-th percentile of that
    distribution.  This means:
      - Keys with historically reliable, low-latency producers get tight
        watermarks → lower output latency.
      - Keys known to produce stragglers automatically get looser watermarks
        → fewer dropped late events.

    The global watermark is min(per-key watermarks), so a single slow key
    does not block all keys.  A ``min_lag`` floor prevents the watermark from
    advancing too aggressively when a key has few observations.
    """

    def __init__(
        self,
        percentile: float = 95.0,
        window_size: int = 100,
        min_lag: float = 1.0,
        max_lag: float = 3600.0,
        global_fallback_lag: float = 60.0,
    ) -> None:
        super().__init__()
        if not (0 < percentile <= 100):
            raise ValueError("percentile must be in (0, 100]")
        self.percentile = percentile
        self.window_size = window_size
        self.min_lag = min_lag
        self.max_lag = max_lag
        self.global_fallback_lag = global_fallback_lag

        self._key_stats: dict[str, _KeyStats] = defaultdict(
            lambda: _KeyStats(window_size, percentile)
        )
        self._per_key_watermarks: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(self, event: Event) -> float:
        stats = self._key_stats[event.key]
        stats.observe(event)

        lag = min(max(stats.lag, self.min_lag), self.max_lag)
        key_wm = stats.max_event_time - lag
        self._per_key_watermarks[event.key] = key_wm

        # Global watermark = minimum across all keys (slowest-key bound)
        self._watermark = min(self._per_key_watermarks.values())
        return self._watermark

    def watermark_for_key(self, key: str) -> float:
        return self._per_key_watermarks.get(key, float("-inf"))

    def lag_for_key(self, key: str) -> float:
        stats = self._key_stats.get(key)
        if stats is None or stats.sample_count == 0:
            return self.global_fallback_lag
        return min(max(stats.lag, self.min_lag), self.max_lag)

    def stats_summary(self) -> dict[str, dict]:
        out = {}
        for key, stats in self._key_stats.items():
            out[key] = {
                "samples": stats.sample_count,
                f"p{self.percentile:.0f}_lag": stats.lag,
                "effective_lag": self.lag_for_key(key),
                "watermark": self._per_key_watermarks.get(key, float("-inf")),
            }
        return out

    def __repr__(self) -> str:
        return (
            f"DynamicPerKeyWatermark(p={self.percentile}, "
            f"window={self.window_size}, "
            f"keys={len(self._key_stats)}, "
            f"global_wm={self._watermark:.3f})"
        )
