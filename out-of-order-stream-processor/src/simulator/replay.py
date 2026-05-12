from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import Optional, Iterator

from ..event import Event, WindowResult, LateEvent
from ..processor import StreamProcessor
from ..watermarks.base import Watermark
from ..windows.base import Window
from ..policies.base import LateDataPolicy


@dataclass
class ReplayConfig:
    """Configuration for a single replay run."""

    name: str
    watermark: Watermark
    window: Window
    late_policy: LateDataPolicy
    description: str = ""


@dataclass
class ReplayMetrics:
    """Collected metrics from one replay run."""

    config_name: str
    total_events: int = 0
    late_events: int = 0
    dropped_events: int = 0
    restated_windows: int = 0
    side_output_events: int = 0
    windows_emitted: int = 0
    # How many events were captured in at least one emitted (non-late) window
    events_in_windows: int = 0
    # Average time from window_end to emit_time (processing latency)
    avg_output_latency: float = 0.0
    # Completeness: fraction of total events that appeared in a final window result
    completeness: float = 0.0
    # Wall-clock time to process the entire stream
    wall_time_seconds: float = 0.0

    results: list[WindowResult] = field(default_factory=list, repr=False)
    late_records: list[LateEvent] = field(default_factory=list, repr=False)

    def summary(self) -> str:
        lines = [
            f"Strategy       : {self.config_name}",
            f"Events in      : {self.total_events}",
            f"Late events    : {self.late_events} "
            f"({100*self.late_events/max(self.total_events,1):.1f}%)",
            f"Dropped        : {self.dropped_events}",
            f"Side-output    : {self.side_output_events}",
            f"Restatements   : {self.restated_windows}",
            f"Windows emitted: {self.windows_emitted}",
            f"Completeness   : {100*self.completeness:.2f}%",
            f"Avg out latency: {self.avg_output_latency:.3f}s",
            f"Wall time      : {self.wall_time_seconds*1000:.1f}ms",
        ]
        return "\n".join(lines)


class StreamReplay:
    """
    Replays a fixed historical event stream through a StreamProcessor.

    The replay clock is *logical* — processing_time for each event is set to
    the event's original processing_time from the historical record, not the
    wall clock.  This makes results deterministic and comparable across runs.

    If the historical stream only contains event_times, you can supply a
    ``latency_fn`` to synthesize processing_times.
    """

    def __init__(
        self,
        events: list[Event],
        sort_by_processing_time: bool = True,
    ) -> None:
        if sort_by_processing_time:
            self._events = sorted(events, key=lambda e: e.processing_time)
        else:
            self._events = list(events)

    @property
    def event_count(self) -> int:
        return len(self._events)

    def run(self, config: ReplayConfig) -> ReplayMetrics:
        """Run a single replay with the given configuration."""
        processor = StreamProcessor(
            watermark=config.watermark,
            window=config.window,
            late_policy=config.late_policy,
        )
        # Reset watermark state between runs
        config.watermark.reset()

        metrics = ReplayMetrics(config_name=config.name)
        t0 = time.perf_counter()

        for event in self._events:
            results, lates = processor.process(event)
            metrics.results.extend(results)
            metrics.late_records.extend(lates)

        # Flush remaining windows
        final = processor.flush()
        metrics.results.extend(final)

        metrics.wall_time_seconds = time.perf_counter() - t0

        # --- Compute metrics ---
        stats = processor.stats()
        metrics.total_events = stats["processed"]
        metrics.late_events = stats["late_total"]
        metrics.dropped_events = stats["late_dropped"]
        metrics.restated_windows = stats["late_restated"]
        metrics.side_output_events = stats["late_side_output"]
        metrics.windows_emitted = len(metrics.results)

        # Events covered by at least one window result (excluding restatements
        # to avoid double-counting)
        final_results = [r for r in metrics.results if not r.is_restatement]
        covered_ids: set[int] = set()
        latencies: list[float] = []
        for r in final_results:
            for e in r.events:
                covered_ids.add(id(e))
            latencies.append(r.latency_seconds)

        metrics.events_in_windows = len(covered_ids)
        metrics.completeness = (
            metrics.events_in_windows / metrics.total_events
            if metrics.total_events > 0
            else 0.0
        )
        metrics.avg_output_latency = (
            sum(latencies) / len(latencies) if latencies else 0.0
        )

        return metrics
