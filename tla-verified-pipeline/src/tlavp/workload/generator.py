"""Synthetic event-log generators.

`healthy_stream`: well-behaved pipeline — every record flows through all
stages in order.

`buggy_stream` variants: each deliberately introduces a specific bug
(orphan warehouse load, missing flink consume, etc.) so the runtime
monitor can be tested for detection.
"""

from __future__ import annotations

import random
from typing import Any


def _record(i: int, group: str = "A", value: int = 1) -> tuple[int, str, int]:
    return (i, group, value)


def healthy_stream(n_records: int = 5, seed: int = 0) -> list[dict[str, Any]]:
    """A clean run: insert → publish → consume → load → push.

    Each step is generated for each record, in order.
    """
    rng = random.Random(seed)
    events: list[dict[str, Any]] = []
    records = [_record(i, rng.choice(["A", "B"])) for i in range(n_records)]
    for r in records:
        events.append({"action": "pg_insert",        "record": r})
        events.append({"action": "debezium_publish", "record": r})
        events.append({"action": "flink_consume"})
        events.append({"action": "warehouse_load",   "record": r})
        events.append({"action": "reverse_etl",      "record": r})
    return events


def buggy_stream(bug: str, n_records: int = 5) -> list[dict[str, Any]]:
    """Various injected-bug scenarios. Bugs:

      - "kafka_lag":           publish n records without any Flink consumes
      - "orphan_warehouse":    warehouse_load without prior consume (caught by
                               warehouse_load precondition); resulting state
                               still satisfies subset invariants — so we
                               inject by direct manipulation below in tests
      - "lost_publish":        pg_insert without subsequent publish
      - "double_publish":      publish same record twice (caught by precondition)
    """
    records = [_record(i) for i in range(n_records)]
    if bug == "kafka_lag":
        events = []
        for r in records:
            events.append({"action": "pg_insert", "record": r})
            events.append({"action": "debezium_publish", "record": r})
        return events
    if bug == "lost_publish":
        events = []
        for r in records:
            events.append({"action": "pg_insert", "record": r})
        return events
    if bug == "double_publish":
        events = []
        r = records[0]
        events.append({"action": "pg_insert", "record": r})
        events.append({"action": "debezium_publish", "record": r})
        events.append({"action": "debezium_publish", "record": r})   # no-op (precondition)
        return events
    raise ValueError(f"unknown bug: {bug}")
