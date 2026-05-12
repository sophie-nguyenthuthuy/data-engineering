from __future__ import annotations
import time
from collections import defaultdict
from typing import Callable, Optional

from .event import Event, LateEvent, WindowResult
from .watermarks.base import Watermark
from .watermarks.fixed import FixedLagWatermark
from .windows.base import Window, WindowAssignment
from .windows.tumbling import TumblingWindow
from .windows.session import SessionWindow
from .policies.base import LateDataPolicy
from .policies.drop import DropPolicy


# (key, window) → list[Event]
_Buffer = dict[tuple[str, WindowAssignment], list[Event]]


class StreamProcessor:
    """
    Core stream processor for out-of-order event-time streams.

    Responsibilities
    ----------------
    1. Maintain a watermark (pluggable strategy).
    2. Assign each event to one or more windows.
    3. Buffer events until the watermark passes their window's end.
    4. Emit WindowResults when windows close.
    5. Apply the configured LateDataPolicy for events that miss the watermark.
    6. Handle session-window merging per key.

    Usage
    -----
    ::

        processor = StreamProcessor(
            watermark=DynamicPerKeyWatermark(percentile=95),
            window=TumblingWindow(size_seconds=60),
            late_policy=SideOutputPolicy(),
        )
        for event in stream:
            results, late = processor.process(event)
            for r in results:
                publish(r)

        # Flush remaining windows at end of stream
        for r in processor.flush():
            publish(r)
    """

    def __init__(
        self,
        watermark: Optional[Watermark] = None,
        window: Optional[Window] = None,
        late_policy: Optional[LateDataPolicy] = None,
        result_callback: Optional[Callable[[WindowResult], None]] = None,
        late_callback: Optional[Callable[[LateEvent], None]] = None,
    ) -> None:
        self.watermark = watermark or FixedLagWatermark(lag_seconds=30.0)
        self.window = window or TumblingWindow(size_seconds=60.0)
        self.late_policy = late_policy or DropPolicy()
        self.result_callback = result_callback
        self.late_callback = late_callback

        # (key, WindowAssignment) → sorted list of events
        self._buffer: _Buffer = defaultdict(list)

        # For session windows: per-key list of provisional windows
        self._session_windows: dict[str, list[WindowAssignment]] = defaultdict(list)

        self._emitted_results: list[WindowResult] = []
        self._late_events: list[LateEvent] = []
        self._processed_count: int = 0

    # ------------------------------------------------------------------
    # Main API
    # ------------------------------------------------------------------

    def process(self, event: Event) -> tuple[list[WindowResult], list[LateEvent]]:
        """Ingest one event.  Returns (results_emitted, late_records)."""
        self._processed_count += 1
        new_watermark = self.watermark.update(event)

        windows = self._assign_windows(event)
        results: list[WindowResult] = []
        lates: list[LateEvent] = []

        for win in windows:
            buf_key = (event.key, win)
            if self.watermark.is_late(event) and event.event_time < win.end:
                # Event is late for this window
                existing = list(self._buffer.get(buf_key, []))
                r, l = self.late_policy.handle(
                    event, win, existing, new_watermark
                )
                results.extend(r)
                lates.extend(l)
            else:
                self._buffer[buf_key].append(event)

        # Check whether any buffered windows can now be closed
        closed = self._close_windows(new_watermark)
        results.extend(closed)

        self._emitted_results.extend(results)
        self._late_events.extend(lates)

        for r in results:
            if self.result_callback:
                self.result_callback(r)
        for le in lates:
            if self.late_callback:
                self.late_callback(le)

        return results, lates

    def flush(self) -> list[WindowResult]:
        """Close all remaining buffered windows (end-of-stream)."""
        results = []
        for (key, win), events in list(self._buffer.items()):
            if events:
                r = self._emit_window(key, win, events)
                results.append(r)
        self._buffer.clear()
        self._session_windows.clear()
        self._emitted_results.extend(results)
        for r in results:
            if self.result_callback:
                self.result_callback(r)
        return results

    # ------------------------------------------------------------------
    # Properties / inspection
    # ------------------------------------------------------------------

    @property
    def current_watermark(self) -> float:
        return self.watermark.current

    @property
    def emitted_results(self) -> list[WindowResult]:
        return list(self._emitted_results)

    @property
    def late_events(self) -> list[LateEvent]:
        return list(self._late_events)

    @property
    def processed_count(self) -> int:
        return self._processed_count

    @property
    def buffered_window_count(self) -> int:
        return sum(1 for events in self._buffer.values() if events)

    def stats(self) -> dict:
        total_late = len(self._late_events)
        dropped = sum(
            1 for le in self._late_events if le.policy_applied == "drop"
        )
        restated = sum(
            1 for le in self._late_events if le.policy_applied == "restate"
        )
        side = sum(
            1 for le in self._late_events if le.policy_applied == "side_output"
        )
        return {
            "processed": self._processed_count,
            "emitted_windows": len(self._emitted_results),
            "late_total": total_late,
            "late_dropped": dropped,
            "late_restated": restated,
            "late_side_output": side,
            "buffered_windows": self.buffered_window_count,
            "watermark": self.watermark.current,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _assign_windows(self, event: Event) -> list[WindowAssignment]:
        if not self.window.is_session_window():
            return self.window.assign(event.event_time)

        # Session: assign provisional, then merge per key
        provisional = self.window.assign(event.event_time)
        key_wins = self._session_windows[event.key]
        key_wins.extend(provisional)
        merged = SessionWindow.merge(key_wins)
        self._session_windows[event.key] = merged

        # Return only the merged window(s) that contain this event
        return [w for w in merged if w.contains(event.event_time) or
                w.start <= event.event_time <= w.end]

    def _close_windows(self, watermark: float) -> list[WindowResult]:
        results = []
        to_delete = []

        for (key, win), events in self._buffer.items():
            if win.end <= watermark and events:
                results.append(self._emit_window(key, win, events))
                to_delete.append((key, win))

                # Remove session window tracking entry
                if self.window.is_session_window() and key in self._session_windows:
                    self._session_windows[key] = [
                        w for w in self._session_windows[key] if w != win
                    ]

        for k in to_delete:
            del self._buffer[k]

        return results

    @staticmethod
    def _emit_window(
        key: str, win: WindowAssignment, events: list[Event]
    ) -> WindowResult:
        return WindowResult(
            window_start=win.start,
            window_end=win.end,
            key=key,
            events=sorted(events, key=lambda e: e.event_time),
            emit_time=time.time(),
            is_restatement=False,
        )
