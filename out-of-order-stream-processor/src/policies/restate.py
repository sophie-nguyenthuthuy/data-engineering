from __future__ import annotations
import time
from ..event import Event, LateEvent, WindowResult
from ..windows.base import WindowAssignment
from .base import LateDataPolicy


class RestatePolicy(LateDataPolicy):
    """
    Reopens the closed window and emits a corrected (restatement) result.

    The restatement includes *all* events seen for that window — the original
    set plus this late-arriving event.  Downstream consumers must be
    idempotent (keyed on window boundaries) to handle corrections.

    ``max_lateness`` caps how late an event can be before it is dropped
    instead of triggering a restatement.  Set to ``float('inf')`` to accept
    arbitrarily late events.
    """

    def __init__(self, max_lateness: float = 3600.0) -> None:
        self.max_lateness = max_lateness

    def handle(
        self,
        event: Event,
        window: WindowAssignment,
        buffered_events: list[Event],
        current_watermark: float,
    ) -> tuple[list[WindowResult], list[LateEvent]]:
        lateness = current_watermark - event.event_time
        record = LateEvent(
            event=event,
            assigned_window_start=window.start,
            assigned_window_end=window.end,
            watermark_at_arrival=current_watermark,
            policy_applied=self.name,
        )

        if lateness > self.max_lateness:
            record.policy_applied = "restate->drop(too_late)"
            return [], [record]

        all_events = sorted(buffered_events + [event], key=lambda e: e.event_time)
        result = WindowResult(
            window_start=window.start,
            window_end=window.end,
            key=event.key,
            events=all_events,
            emit_time=time.time(),
            is_restatement=True,
        )
        return [result], [record]

    @property
    def name(self) -> str:
        return "restate"
