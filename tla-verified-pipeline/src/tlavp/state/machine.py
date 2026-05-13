"""State variables (mirror TLA+ VARIABLES block).

The state is the cartesian product of:
  - pg:        set of records inserted into Postgres
  - kafka:     in-order list of records published by Debezium (offsets implicit)
  - flink_sum: running aggregate consumed by Flink (one per group key)
  - warehouse: set of records written to the data warehouse
  - rev_etl:   set of records pushed downstream to a SaaS target

Concretely each "record" is a tuple `(id, group, value)` where `group` is
the key Flink aggregates over.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

# Record type used throughout
Record = tuple[Any, Any, Any]    # (id, group, value)


@dataclass
class State:
    """Mutable state. Direct field access for clarity in tests / replays."""

    pg: set[Record] = field(default_factory=set)
    kafka: list[Record] = field(default_factory=list)
    flink_sum: dict[Any, int] = field(default_factory=lambda: defaultdict(int))
    warehouse: set[Record] = field(default_factory=set)
    rev_etl: set[Record] = field(default_factory=set)
    # Bookkeeping (not part of TLA+ spec but useful for testing)
    kafka_consumed: list[Record] = field(default_factory=list)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "pg": set(self.pg),
                "kafka": list(self.kafka),
                "kafka_lag": len(self.kafka),
                "flink_sum": dict(self.flink_sum),
                "warehouse": set(self.warehouse),
                "rev_etl": set(self.rev_etl),
            }


@dataclass
class StateMachine:
    """Wrapper around `State` with action methods + step counter."""

    state: State = field(default_factory=State)
    step_count: int = 0
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def step(self) -> int:
        with self._lock:
            self.step_count += 1
            return self.step_count

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self.state.snapshot()


__all__ = ["Record", "State", "StateMachine"]
