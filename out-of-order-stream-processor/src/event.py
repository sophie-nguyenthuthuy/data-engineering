from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional
import time


@dataclass(order=True)
class Event:
    """A single event in the stream."""

    event_time: float          # epoch seconds (when the event actually occurred)
    key: str                   # partition / grouping key
    value: Any = field(compare=False)
    processing_time: float = field(default_factory=time.time, compare=False)
    sequence_id: int = field(default=0, compare=False)

    @property
    def ingestion_latency(self) -> float:
        """Seconds between event_time and when it was ingested."""
        return self.processing_time - self.event_time

    def __repr__(self) -> str:
        return (
            f"Event(key={self.key!r}, event_time={self.event_time:.3f}, "
            f"value={self.value!r}, latency={self.ingestion_latency:.3f}s)"
        )


@dataclass
class WindowResult:
    """Result emitted when a window closes."""

    window_start: float
    window_end: float
    key: str
    events: list[Event]
    emit_time: float = field(default_factory=time.time)
    is_restatement: bool = False

    @property
    def count(self) -> int:
        return len(self.events)

    @property
    def values(self) -> list[Any]:
        return [e.value for e in self.events]

    @property
    def latency_seconds(self) -> float:
        """Time from window_end to when result was emitted."""
        return self.emit_time - self.window_end

    def __repr__(self) -> str:
        tag = " [RESTATEMENT]" if self.is_restatement else ""
        return (
            f"WindowResult(key={self.key!r}, "
            f"[{self.window_start:.1f}, {self.window_end:.1f}), "
            f"count={self.count}, latency={self.latency_seconds:.3f}s{tag})"
        )


@dataclass
class LateEvent:
    """An event that arrived after its window's watermark."""

    event: Event
    assigned_window_start: float
    assigned_window_end: float
    watermark_at_arrival: float
    policy_applied: str

    @property
    def lateness(self) -> float:
        return self.watermark_at_arrival - self.event.event_time
