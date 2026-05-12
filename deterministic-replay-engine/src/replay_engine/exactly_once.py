"""Exactly-once delivery tracker.

Maintains a persistent in-memory record of which events have been processed
(keyed by their content hash). Detects violations:

- DUPLICATE_DELIVERY: same event processed more than once
- MISSING_PREDECESSOR: an event was processed before its causal predecessor
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any

from .event import Event
from .vector_clock import VectorClock


class ViolationKind(Enum):
    DUPLICATE_DELIVERY = auto()
    MISSING_PREDECESSOR = auto()
    OUT_OF_ORDER = auto()


@dataclass
class ExactlyOnceViolation:
    kind: ViolationKind
    event: Event
    detail: str

    def __str__(self) -> str:
        return f"[{self.kind.name}] event={self.event.event_id!r}: {self.detail}"


class ExactlyOnceTracker:
    """Tracks processed events and detects exactly-once violations.

    Call ``track(event)`` in the order events are replayed.  Violations are
    accumulated and can be retrieved via ``violations``.
    """

    def __init__(self) -> None:
        # event_id -> content_hash for all processed events
        self._processed: dict[str, str] = {}
        # producer_id -> max sequence_num seen
        self._max_seq: dict[str, int] = {}
        self._violations: list[ExactlyOnceViolation] = []

    def track(self, event: Event) -> list[ExactlyOnceViolation]:
        """Process ``event`` and return any new violations detected."""
        new_violations: list[ExactlyOnceViolation] = []

        # 1. Duplicate check
        if event.event_id in self._processed:
            v = ExactlyOnceViolation(
                kind=ViolationKind.DUPLICATE_DELIVERY,
                event=event,
                detail=(
                    f"Event already processed with hash "
                    f"{self._processed[event.event_id]!r}"
                ),
            )
            new_violations.append(v)
            self._violations.append(v)
            return new_violations  # don't update state

        # 2. Causal predecessor check: for each producer P that this event
        #    depends on, we must have already seen P's event at that sequence.
        for producer_id, required_seq in event.vector_clock.clocks.items():
            if producer_id == event.producer_id:
                continue  # own-clock entry is not a causal predecessor
            seen_seq = self._max_seq.get(producer_id, -1)
            if seen_seq < required_seq:
                v = ExactlyOnceViolation(
                    kind=ViolationKind.MISSING_PREDECESSOR,
                    event=event,
                    detail=(
                        f"Depends on {producer_id!r} seq>={required_seq} "
                        f"but only seq={seen_seq} seen so far"
                    ),
                )
                new_violations.append(v)
                self._violations.append(v)

        # 3. Out-of-order within same producer
        prev_seq = self._max_seq.get(event.producer_id, -1)
        if event.sequence_num != prev_seq + 1:
            v = ExactlyOnceViolation(
                kind=ViolationKind.OUT_OF_ORDER,
                event=event,
                detail=(
                    f"Producer {event.producer_id!r}: expected seq {prev_seq + 1}, "
                    f"got {event.sequence_num}"
                ),
            )
            new_violations.append(v)
            self._violations.append(v)

        # Commit
        self._processed[event.event_id] = event.content_hash()
        self._max_seq[event.producer_id] = max(
            self._max_seq.get(event.producer_id, -1), event.sequence_num
        )

        return new_violations

    def violations(self) -> list[ExactlyOnceViolation]:
        return list(self._violations)

    def processed_count(self) -> int:
        return len(self._processed)

    def report(self) -> dict[str, Any]:
        by_kind: dict[str, list[str]] = {}
        for v in self._violations:
            by_kind.setdefault(v.kind.name, []).append(v.event.event_id)
        return {
            "processed_count": self.processed_count(),
            "total_violations": len(self._violations),
            "by_kind": by_kind,
        }
