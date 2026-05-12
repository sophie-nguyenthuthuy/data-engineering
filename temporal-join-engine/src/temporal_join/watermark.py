"""Per-stream watermark tracking."""
from __future__ import annotations


class WatermarkTracker:
    """
    Tracks the per-stream event-time watermark.

    watermark = max_event_time_seen - lateness_bound

    An event is:
    - on-time              if event_time >= max_seen (advances the watermark)
    - reclaimably late     if watermark <= event_time < max_seen
                           (out-of-order but within the lateness budget — can trigger corrections)
    - irreparably late     if event_time < watermark
                           (too old to be useful; discarded by default)
    """

    def __init__(self, lateness_bound: int) -> None:
        if lateness_bound < 0:
            raise ValueError("lateness_bound must be >= 0")
        self.lateness_bound = lateness_bound
        self._max_seen: int = -(2 ** 62)
        self._watermark: int = -(2 ** 62)

    def observe(self, event_time: int) -> None:
        """Update internal state based on a newly observed event timestamp."""
        if event_time > self._max_seen:
            self._max_seen = event_time
            self._watermark = self._max_seen - self.lateness_bound

    @property
    def watermark(self) -> int:
        return self._watermark

    @property
    def max_seen(self) -> int:
        return self._max_seen

    def is_irreparably_late(self, event_time: int) -> bool:
        """True when event_time is older than the watermark — no correction possible."""
        return event_time < self._watermark

    def is_reclaimably_late(self, event_time: int) -> bool:
        """True when the event is out-of-order but still within the lateness window."""
        return self._watermark <= event_time < self._max_seen

    def is_on_time(self, event_time: int) -> bool:
        return event_time >= self._max_seen

    def advance_to(self, watermark: int) -> None:
        """Externally advance the watermark (e.g. from a watermark event or punctuation)."""
        if watermark > self._watermark:
            self._watermark = watermark
        if watermark + self.lateness_bound > self._max_seen:
            self._max_seen = watermark + self.lateness_bound
