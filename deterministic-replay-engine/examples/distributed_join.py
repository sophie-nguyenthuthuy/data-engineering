"""Example: distributed join — detecting non-determinism from a random UDF.

Two producers (orders, payments) emit events.  A join UDF is intentionally
made non-deterministic (adds a random salt) so the detector catches it.
"""

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from replay_engine import Event, EventLog, ReplayEngine, VectorClock
from replay_engine.udf_detector import NonDeterminismError


def build_join_log() -> EventLog:
    log = EventLog()

    log.append(Event("ord0", "orders",   0, 100.0, VectorClock({"orders": 0}),   {"order_id": 1, "amount": 99.0}))
    log.append(Event("ord1", "orders",   1, 101.0, VectorClock({"orders": 1}),   {"order_id": 2, "amount": 250.0}))
    log.append(Event("pay0", "payments", 0, 102.0, VectorClock({"orders": 0, "payments": 0}), {"order_id": 1, "status": "settled"}))
    log.append(Event("pay1", "payments", 1, 103.0, VectorClock({"orders": 1, "payments": 1}), {"order_id": 2, "status": "pending"}))

    return log


def deterministic_join(event: Event) -> dict:
    return {"event_id": event.event_id, "producer": event.producer_id, **event.payload}


def flaky_join(event: Event) -> dict:
    """Simulates a UDF that reads wall-clock time or random state — non-deterministic."""
    return {**deterministic_join(event), "salt": random.random()}


def main():
    log = build_join_log()

    print("=== Deterministic join (should pass) ===")
    engine = ReplayEngine(udfs={"join": deterministic_join}, udf_runs=3)
    result = engine.replay(log)
    print(result.summary())
    print(f"Success: {result.success}\n")

    print("=== Non-deterministic join (should fail) ===")
    engine2 = ReplayEngine(udfs={"join": flaky_join}, udf_runs=2)
    result2 = engine2.replay(log)
    print(result2.summary())
    violations = result2.udf_reports.get("join", {}).get("violation_event_ids", [])
    print(f"Success: {result2.success}")
    print(f"Non-determinism detected in events: {violations}")


if __name__ == "__main__":
    main()
