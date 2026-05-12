from __future__ import annotations
from .base import Window, WindowAssignment


class SessionWindow(Window):
    """
    Gap-based session windows.

    A session ends when no event for the same key arrives within
    ``gap_seconds``.  Each event is initially assigned a provisional window
    [event_time, event_time + gap]; the processor merges overlapping
    provisional windows per key as events arrive.

    Session window boundaries are *dynamic* — they change as new events
    arrive — so the processor must handle merging explicitly.
    """

    def __init__(self, gap_seconds: float) -> None:
        if gap_seconds <= 0:
            raise ValueError("gap_seconds must be positive")
        self.gap_seconds = gap_seconds

    def assign(self, event_time: float) -> list[WindowAssignment]:
        # Provisional single-event window; processor merges them.
        return [
            WindowAssignment(
                start=event_time, end=event_time + self.gap_seconds
            )
        ]

    def is_session_window(self) -> bool:
        return True

    @staticmethod
    def merge(
        windows: list[WindowAssignment],
    ) -> list[WindowAssignment]:
        """Merge a list of overlapping/touching provisional windows."""
        if not windows:
            return []
        sorted_wins = sorted(windows)
        merged = [sorted_wins[0]]
        for w in sorted_wins[1:]:
            last = merged[-1]
            if w.start <= last.end:
                merged[-1] = WindowAssignment(
                    start=last.start, end=max(last.end, w.end)
                )
            else:
                merged.append(w)
        return merged

    def __repr__(self) -> str:
        return f"SessionWindow(gap={self.gap_seconds}s)"
