"""UDF non-determinism detector.

Wraps a user-defined function and replays it multiple times with the same input.
If the output differs across runs, it raises NonDeterminismError and records
which events triggered divergence.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .event import Event


class NonDeterminismError(Exception):
    """Raised when a UDF produces different outputs for the same input."""

    def __init__(self, udf_name: str, event_id: str, run1: Any, run2: Any) -> None:
        self.udf_name = udf_name
        self.event_id = event_id
        self.run1 = run1
        self.run2 = run2
        super().__init__(
            f"UDF {udf_name!r} is non-deterministic for event {event_id!r}: "
            f"run1={run1!r} != run2={run2!r}"
        )


@dataclass
class UDFRecord:
    """Stores the input hash, output, and latency for one UDF invocation."""

    event_id: str
    input_hash: str
    output: Any
    latency_ms: float
    run_index: int


@dataclass
class UDFDetector:
    """Wraps a UDF and detects non-determinism by running it ``num_runs`` times
    per event and comparing all outputs.

    Usage::

        detector = UDFDetector("my_transform", my_fn, num_runs=3)
        output = detector(event)          # raises NonDeterminismError if diverges
        report = detector.report()        # summary dict
    """

    name: str
    fn: Callable[[Event], Any]
    num_runs: int = 2
    # Records keyed by event_id -> list of UDFRecord (one per run)
    _records: dict[str, list[UDFRecord]] = field(default_factory=dict, init=False, repr=False)
    _violations: list[NonDeterminismError] = field(default_factory=list, init=False, repr=False)

    def __call__(self, event: Event) -> Any:
        input_hash = self._hash_input(event)
        runs: list[UDFRecord] = []

        for run_i in range(self.num_runs):
            t0 = time.perf_counter()
            output = self.fn(event)
            latency_ms = (time.perf_counter() - t0) * 1000
            runs.append(
                UDFRecord(
                    event_id=event.event_id,
                    input_hash=input_hash,
                    output=output,
                    latency_ms=latency_ms,
                    run_index=run_i,
                )
            )

        self._records[event.event_id] = runs

        # Check determinism: all outputs must be equal.
        baseline = runs[0].output
        for r in runs[1:]:
            if not self._outputs_equal(baseline, r.output):
                err = NonDeterminismError(self.name, event.event_id, baseline, r.output)
                self._violations.append(err)
                raise err

        return baseline

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    def violations(self) -> list[NonDeterminismError]:
        return list(self._violations)

    def report(self) -> dict[str, Any]:
        total_events = len(self._records)
        total_violations = len(self._violations)
        avg_latency: float | None = None
        if self._records:
            all_latencies = [r.latency_ms for runs in self._records.values() for r in runs]
            avg_latency = sum(all_latencies) / len(all_latencies)

        return {
            "udf_name": self.name,
            "num_runs_per_event": self.num_runs,
            "total_events_processed": total_events,
            "total_violations": total_violations,
            "violation_event_ids": [v.event_id for v in self._violations],
            "avg_latency_ms": avg_latency,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    @staticmethod
    def _hash_input(event: Event) -> str:
        return event.content_hash()

    @staticmethod
    def _outputs_equal(a: Any, b: Any) -> bool:
        try:
            return json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
        except (TypeError, ValueError):
            return a == b
