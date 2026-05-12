"""Event and EventLog data structures."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from .vector_clock import VectorClock


@dataclass
class Event:
    """A single event emitted by one producer in a distributed pipeline."""

    event_id: str
    producer_id: str
    # Logical sequence number scoped to this producer (0-based, monotone).
    sequence_num: int
    # Wall-clock timestamp (epoch seconds, used only for display / tie-breaking).
    timestamp: float
    # Causal dependencies: maps producer_id -> sequence_num of the last event
    # from that producer that causally precedes this one.
    vector_clock: VectorClock
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Ensure the producer's own clock entry is at least sequence_num.
        self.vector_clock = VectorClock(
            {**self.vector_clock.clocks, self.producer_id: self.sequence_num}
        )

    # ------------------------------------------------------------------
    # Stable content hash (excludes wall-clock timestamp so that replays
    # with adjusted timestamps still match).
    # ------------------------------------------------------------------
    def content_hash(self) -> str:
        canonical = json.dumps(
            {
                "event_id": self.event_id,
                "producer_id": self.producer_id,
                "sequence_num": self.sequence_num,
                "vector_clock": self.vector_clock.clocks,
                "payload": self.payload,
            },
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "producer_id": self.producer_id,
            "sequence_num": self.sequence_num,
            "timestamp": self.timestamp,
            "vector_clock": self.vector_clock.clocks,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Event:
        return cls(
            event_id=d["event_id"],
            producer_id=d["producer_id"],
            sequence_num=d["sequence_num"],
            timestamp=d["timestamp"],
            vector_clock=VectorClock(d.get("vector_clock", {})),
            payload=d.get("payload", {}),
        )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"Event({self.event_id!r}, producer={self.producer_id!r}, "
            f"seq={self.sequence_num}, vc={self.vector_clock})"
        )


class EventLog:
    """Ordered collection of events parsed from a pipeline log file."""

    def __init__(self, events: list[Event] | None = None) -> None:
        self._events: list[Event] = events or []
        self._by_id: dict[str, Event] = {e.event_id: e for e in self._events}

    def append(self, event: Event) -> None:
        if event.event_id in self._by_id:
            raise ValueError(f"Duplicate event_id: {event.event_id!r}")
        self._events.append(event)
        self._by_id[event.event_id] = event

    def __len__(self) -> int:
        return len(self._events)

    def __iter__(self):
        return iter(self._events)

    def __getitem__(self, event_id: str) -> Event:
        return self._by_id[event_id]

    def producers(self) -> set[str]:
        return {e.producer_id for e in self._events}

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------
    def to_jsonl(self) -> str:
        return "\n".join(json.dumps(e.to_dict()) for e in self._events)

    @classmethod
    def from_jsonl(cls, text: str) -> EventLog:
        log = cls()
        for line in text.strip().splitlines():
            if line.strip():
                log.append(Event.from_dict(json.loads(line)))
        return log

    def to_json(self) -> str:
        return json.dumps([e.to_dict() for e in self._events], indent=2)

    @classmethod
    def from_json(cls, text: str) -> EventLog:
        log = cls()
        for d in json.loads(text):
            log.append(Event.from_dict(d))
        return log
