from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..event import Event, LateEvent, WindowResult
    from ..windows.base import WindowAssignment


class LateDataPolicy(ABC):
    """
    Decides what happens when an event arrives after its window's watermark.

    Three built-in strategies:
      - Drop        : discard the event silently
      - Restate     : reopen the window, add the event, emit a correction
      - SideOutput  : route to a separate collection for manual processing
    """

    @abstractmethod
    def handle(
        self,
        event: "Event",
        window: "WindowAssignment",
        buffered_events: list["Event"],
        current_watermark: float,
    ) -> tuple[list["WindowResult"], list["LateEvent"]]:
        """
        Process a late event.

        Returns:
            (results_to_emit, late_event_records)
            results_to_emit : any WindowResults produced (e.g. restatements)
            late_event_records : LateEvent records for audit / side output
        """

    @property
    @abstractmethod
    def name(self) -> str:
        ...
