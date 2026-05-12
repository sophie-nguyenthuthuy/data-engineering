from __future__ import annotations
import math
from .base import Window, WindowAssignment


class TumblingWindow(Window):
    """
    Non-overlapping, fixed-size windows.

    Events with event_time in [k*size, (k+1)*size) land in exactly one window.

        |-- win 0 --|-- win 1 --|-- win 2 --|
        0          10          20          30   (size=10)
    """

    def __init__(self, size_seconds: float, offset: float = 0.0) -> None:
        if size_seconds <= 0:
            raise ValueError("size_seconds must be positive")
        self.size_seconds = size_seconds
        self.offset = offset

    def assign(self, event_time: float) -> list[WindowAssignment]:
        t = event_time - self.offset
        k = math.floor(t / self.size_seconds)
        start = k * self.size_seconds + self.offset
        return [WindowAssignment(start=start, end=start + self.size_seconds)]

    def is_session_window(self) -> bool:
        return False

    def __repr__(self) -> str:
        return f"TumblingWindow(size={self.size_seconds}s)"
