"""Core replay engine: orchestrates causal sort, UDF execution, and tracking."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .causal_order import CausalOrderError, causal_sort, validate_monotone_sequences
from .event import Event, EventLog
from .exactly_once import ExactlyOnceTracker, ExactlyOnceViolation
from .udf_detector import NonDeterminismError, UDFDetector


@dataclass
class StepResult:
    event: Event
    output: Any
    latency_ms: float
    exactly_once_violations: list[ExactlyOnceViolation]
    udf_error: NonDeterminismError | None = None


@dataclass
class ReplayResult:
    ordered_events: list[Event]
    steps: list[StepResult]
    exactly_once_report: dict[str, Any]
    udf_reports: dict[str, dict[str, Any]]
    duration_ms: float
    sequence_errors: list[str]
    success: bool

    def summary(self) -> str:
        lines = [
            f"Replayed {len(self.ordered_events)} events in {self.duration_ms:.1f} ms",
            f"Exactly-once violations: {self.exactly_once_report['total_violations']}",
            f"UDF non-determinism violations: "
            + str(sum(r["total_violations"] for r in self.udf_reports.values())),
        ]
        if self.sequence_errors:
            lines.append(f"Sequence errors: {len(self.sequence_errors)}")
        return "\n".join(lines)


class ReplayEngine:
    """Deterministic replay engine.

    Parameters
    ----------
    udfs:
        Mapping from name to callable. Each UDF receives an ``Event`` and
        returns any JSON-serialisable value.  If omitted, events are replayed
        without transformation.
    udf_runs:
        How many times each UDF is called per event to detect non-determinism.
    stop_on_violation:
        If True, raise immediately on the first exactly-once or UDF violation.
    """

    def __init__(
        self,
        udfs: dict[str, Callable[[Event], Any]] | None = None,
        udf_runs: int = 2,
        stop_on_violation: bool = False,
    ) -> None:
        self._raw_udfs = udfs or {}
        self._udf_runs = udf_runs
        self._stop_on_violation = stop_on_violation

    def replay(self, log: EventLog) -> ReplayResult:
        t_start = time.perf_counter()

        events = list(log)

        # 1. Validate monotone sequences (warn but don't abort)
        seq_errors = validate_monotone_sequences(events)

        # 2. Causal sort
        try:
            ordered = causal_sort(events)
        except CausalOrderError as exc:
            raise RuntimeError(f"Cannot replay: {exc}") from exc

        # 3. Build per-run detectors and tracker
        detectors: dict[str, UDFDetector] = {
            name: UDFDetector(name=name, fn=fn, num_runs=self._udf_runs)
            for name, fn in self._raw_udfs.items()
        }
        tracker = ExactlyOnceTracker()

        steps: list[StepResult] = []
        success = True

        for event in ordered:
            step_t0 = time.perf_counter()
            udf_error: NonDeterminismError | None = None
            output: Any = None

            # Run all UDFs in registration order
            for name, detector in detectors.items():
                try:
                    output = detector(event)
                except NonDeterminismError as err:
                    udf_error = err
                    success = False
                    if self._stop_on_violation:
                        raise

            latency_ms = (time.perf_counter() - step_t0) * 1000

            # Exactly-once tracking
            new_violations = tracker.track(event)
            if new_violations:
                success = False
                if self._stop_on_violation:
                    raise RuntimeError(
                        f"Exactly-once violation: {new_violations[0]}"
                    )

            steps.append(
                StepResult(
                    event=event,
                    output=output,
                    latency_ms=latency_ms,
                    exactly_once_violations=new_violations,
                    udf_error=udf_error,
                )
            )

        duration_ms = (time.perf_counter() - t_start) * 1000

        return ReplayResult(
            ordered_events=ordered,
            steps=steps,
            exactly_once_report=tracker.report(),
            udf_reports={name: d.report() for name, d in detectors.items()},
            duration_ms=duration_ms,
            sequence_errors=seq_errors,
            success=success,
        )
