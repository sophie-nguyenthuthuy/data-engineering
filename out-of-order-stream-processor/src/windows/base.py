from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator


@dataclass(frozen=True, order=True)
class WindowAssignment:
    """A half-open time interval [start, end)."""

    start: float
    end: float

    @property
    def duration(self) -> float:
        return self.end - self.start

    def contains(self, event_time: float) -> bool:
        return self.start <= event_time < self.end

    def __repr__(self) -> str:
        return f"[{self.start:.1f}, {self.end:.1f})"


class Window(ABC):
    """
    Strategy that maps an event_time to one or more WindowAssignments.

    Windows are stateless — they only compute assignments; the processor
    manages buffered events and emits results.
    """

    @abstractmethod
    def assign(self, event_time: float) -> list[WindowAssignment]:
        """Return all windows this event_time belongs to."""

    @abstractmethod
    def is_session_window(self) -> bool:
        """Session windows require special merge logic in the processor."""
