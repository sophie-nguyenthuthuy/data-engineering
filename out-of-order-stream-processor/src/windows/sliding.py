from __future__ import annotations
import math
from .base import Window, WindowAssignment


class SlidingWindow(Window):
    """
    Overlapping windows of fixed size, sliding every ``slide`` seconds.

    An event may belong to ``ceil(size / slide)`` windows simultaneously.

        size=20s, slide=10s:
        |-------- win A --------|
                  |-------- win B --------|
                            |-------- win C --------|
        0        10         20         30         40
    """

    def __init__(self, size_seconds: float, slide_seconds: float) -> None:
        if size_seconds <= 0:
            raise ValueError("size_seconds must be positive")
        if slide_seconds <= 0:
            raise ValueError("slide_seconds must be positive")
        if slide_seconds > size_seconds:
            raise ValueError("slide_seconds must be <= size_seconds")
        self.size_seconds = size_seconds
        self.slide_seconds = slide_seconds

    def assign(self, event_time: float) -> list[WindowAssignment]:
        # Earliest window that could contain event_time ends at
        # the first multiple-of-slide that is > event_time.
        last_start = (
            math.floor(event_time / self.slide_seconds) * self.slide_seconds
        )
        windows = []
        start = last_start
        while start + self.size_seconds > event_time:
            if start <= event_time < start + self.size_seconds:
                windows.append(
                    WindowAssignment(start=start, end=start + self.size_seconds)
                )
            start -= self.slide_seconds
        return sorted(windows)

    def is_session_window(self) -> bool:
        return False

    def __repr__(self) -> str:
        return (
            f"SlidingWindow(size={self.size_seconds}s, "
            f"slide={self.slide_seconds}s)"
        )
