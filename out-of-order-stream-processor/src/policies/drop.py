from __future__ import annotations
import time
from ..event import Event, LateEvent, WindowResult
from ..windows.base import WindowAssignment
from .base import LateDataPolicy


class DropPolicy(LateDataPolicy):
    """
    Silently discards late events.

    Simplest policy — use when late data is rare and correctness of closed
    windows matters less than pipeline simplicity.
    """

    def handle(
        self,
        event: Event,
        window: WindowAssignment,
        buffered_events: list[Event],
        current_watermark: float,
    ) -> tuple[list[WindowResult], list[LateEvent]]:
        record = LateEvent(
            event=event,
            assigned_window_start=window.start,
            assigned_window_end=window.end,
            watermark_at_arrival=current_watermark,
            policy_applied=self.name,
        )
        return [], [record]

    @property
    def name(self) -> str:
        return "drop"
