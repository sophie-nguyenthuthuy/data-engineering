"""DLQ routing.

The :class:`DLQRouter` inspects a *payload* (raw bytes / str / dict)
and either returns the parsed :class:`DebeziumEnvelope` or a
:class:`DLQDecision` describing why the event should be sent to the
dead-letter topic.

Reasons it routes:
  * ``PARSE_ERROR``  — the payload is not valid Debezium JSON.
  * ``MISSING_FIELD`` — a transform raised because of missing data.
  * ``UNKNOWN_OP``    — `op` is not one of ``c|u|d|r``.
  * ``CUSTOM``         — caller-supplied predicate said no.

A real deployment hands the DLQ payload (the original bytes) plus the
reason + reason message to a Kafka producer pointed at the DLQ topic.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from cdc.events.envelope import DebeziumEnvelope
from cdc.events.parse import ParseError, parse_envelope


class DLQReason(str, Enum):
    """Why an event was routed to the DLQ."""

    PARSE_ERROR = "parse_error"
    MISSING_FIELD = "missing_field"
    UNKNOWN_OP = "unknown_op"
    CUSTOM = "custom"


@dataclass(frozen=True, slots=True)
class DLQDecision:
    """Outcome of :meth:`DLQRouter.route`."""

    reason: DLQReason
    message: str
    raw: bytes | str | dict[str, Any] | None = None


Predicate = Callable[[DebeziumEnvelope], bool]


@dataclass
class DLQRouter:
    """Stateless DLQ classifier."""

    custom_check: Predicate | None = None
    custom_message: str = "rejected by custom predicate"
    _counts: dict[DLQReason, int] = field(default_factory=dict, init=False, repr=False)

    def route(self, payload: bytes | str | dict[str, Any]) -> DebeziumEnvelope | DLQDecision:
        """Return the parsed envelope or a DLQ decision."""
        try:
            envelope = parse_envelope(payload)
        except ParseError as exc:
            msg = str(exc)
            return self._record(DLQDecision(self._classify(msg), msg, payload))

        if self.custom_check is not None and not self.custom_check(envelope):
            return self._record(DLQDecision(DLQReason.CUSTOM, self.custom_message, payload))
        return envelope

    @staticmethod
    def _classify(message: str) -> DLQReason:
        if "unknown Debezium op" in message:
            return DLQReason.UNKNOWN_OP
        if message.startswith("invalid JSON") or "must be a JSON object" in message:
            return DLQReason.PARSE_ERROR
        return DLQReason.MISSING_FIELD

    def counts(self) -> dict[DLQReason, int]:
        return dict(self._counts)

    def _record(self, decision: DLQDecision) -> DLQDecision:
        self._counts[decision.reason] = self._counts.get(decision.reason, 0) + 1
        return decision


__all__ = ["DLQDecision", "DLQReason", "DLQRouter"]
