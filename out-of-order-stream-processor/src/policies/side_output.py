from __future__ import annotations
from ..event import Event, LateEvent, WindowResult
from ..windows.base import WindowAssignment
from .base import LateDataPolicy


class SideOutputPolicy(LateDataPolicy):
    """
    Routes late events to a side-output collection instead of the main stream.

    Late events are accessible via ``.side_output`` for downstream handling
    (manual review, separate aggregation pipeline, dead-letter queue, etc.).

    Unlike RestatePolicy this never modifies already-emitted window results.
    """

    def __init__(self) -> None:
        self._side_output: list[LateEvent] = []

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
        self._side_output.append(record)
        return [], [record]

    @property
    def side_output(self) -> list[LateEvent]:
        return list(self._side_output)

    def drain_side_output(self) -> list[LateEvent]:
        out, self._side_output = self._side_output, []
        return out

    @property
    def name(self) -> str:
        return "side_output"
