from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
from ..event import Event


class Watermark(ABC):
    """
    A watermark tracks progress in event time.

    The watermark value W(t) at processing time t asserts: "all events with
    event_time <= W(t) have been observed."  Events arriving with
    event_time <= current_watermark are considered *late*.
    """

    def __init__(self) -> None:
        self._watermark: float = float("-inf")

    @property
    def current(self) -> float:
        return self._watermark

    @abstractmethod
    def update(self, event: Event) -> float:
        """Ingest an event and return the new watermark value."""

    def is_late(self, event: Event) -> bool:
        # Strictly less-than: an event whose event_time equals the current
        # watermark is not late (it may have just advanced the watermark).
        return event.event_time < self._watermark

    def reset(self) -> None:
        self._watermark = float("-inf")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(watermark={self._watermark:.3f})"
