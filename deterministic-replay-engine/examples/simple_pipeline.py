"""Example: three-producer pipeline with a deliberate duplicate event."""

import json
import sys
from pathlib import Path

# Allow running from the repo root without installing.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from replay_engine import Event, EventLog, ReplayEngine, VectorClock
from replay_engine.exactly_once import ViolationKind


def build_log() -> EventLog:
    log = EventLog()

    # Producer A emits 3 events independently.
    log.append(Event("a0", "A", 0, 1_000.0, VectorClock({"A": 0}), {"msg": "A started"}))
    log.append(Event("a1", "A", 1, 1_001.0, VectorClock({"A": 1}), {"msg": "A processed"}))
    log.append(Event("a2", "A", 2, 1_002.0, VectorClock({"A": 2}), {"msg": "A finished"}))

    # Producer B depends on A0.
    log.append(Event("b0", "B", 0, 1_001.5, VectorClock({"A": 0, "B": 0}), {"msg": "B seen A"}))
    log.append(Event("b1", "B", 1, 1_003.0, VectorClock({"A": 1, "B": 1}), {"msg": "B done"}))

    # Producer C depends on both A2 and B1.
    log.append(Event(
        "c0", "C", 0, 1_010.0,
        VectorClock({"A": 2, "B": 1, "C": 0}),
        {"msg": "C joined A+B"},
    ))

    return log


def transform(event: Event):
    """Simple UDF: uppercase the msg payload."""
    return event.payload.get("msg", "").upper()


def main():
    log = build_log()
    print(f"Log has {len(log)} events from producers: {log.producers()}\n")

    engine = ReplayEngine(udfs={"transform": transform}, udf_runs=2)
    result = engine.replay(log)

    print("Causal replay order:")
    for i, step in enumerate(result.steps):
        print(f"  [{i}] {step.event.event_id:4s}  producer={step.event.producer_id}  "
              f"seq={step.event.sequence_num}  output={step.output!r}")

    print(f"\n{result.summary()}")
    print(f"Success: {result.success}")


if __name__ == "__main__":
    main()
