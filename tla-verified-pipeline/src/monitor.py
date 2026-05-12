"""Runtime monitor: replays an event log through the state machine + checks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from .state_machine import (
    State, pg_insert, debezium_publish, flink_consume,
    warehouse_load, reverse_etl,
)
from .invariants import check_all


ACTION_MAP = {
    "pg_insert":         lambda s, e: pg_insert(s, e["record"]),
    "debezium_publish":  lambda s, e: debezium_publish(s, e["record"]),
    "flink_consume":     lambda s, e: flink_consume(s),
    "warehouse_load":    lambda s, e: warehouse_load(s, e["record"]),
    "reverse_etl":       lambda s, e: reverse_etl(s, e["record"]),
}


@dataclass
class Incident:
    step: int
    action: str
    violations: list
    state_snapshot: dict


@dataclass
class Monitor:
    max_lag: int = 1000
    state: State = field(default_factory=State)
    incidents: list = field(default_factory=list)
    _step: int = 0

    def replay(self, events: list[dict]) -> list[Incident]:
        for e in events:
            self._step += 1
            action_fn = ACTION_MAP.get(e["action"])
            if action_fn is None:
                continue
            ok = action_fn(self.state, e)
            violations = check_all(self.state, max_lag=self.max_lag)
            if violations:
                self.incidents.append(Incident(
                    step=self._step,
                    action=e["action"],
                    violations=list(violations),
                    state_snapshot={
                        "pg": set(self.state.pg),
                        "kafka": list(self.state.kafka),
                        "flink_sum": self.state.flink_sum,
                        "warehouse": set(self.state.warehouse),
                        "rev_etl": set(self.state.rev_etl),
                    },
                ))
        return self.incidents


__all__ = ["Monitor", "Incident"]
