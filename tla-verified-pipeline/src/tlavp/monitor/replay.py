"""Runtime monitor: drives the state machine from an event log, checks
invariants after every step, emits incidents on violations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from tlavp.invariants.liveness import EventualDeliveryWatcher
from tlavp.invariants.safety import check_all
from tlavp.monitor.alerts import AlertSink, ListAlertSink
from tlavp.state.actions import (
    debezium_publish,
    flink_consume,
    pg_insert,
    reverse_etl,
    warehouse_load,
)
from tlavp.state.machine import StateMachine


@dataclass(frozen=True, slots=True)
class Incident:
    step: int
    action: str
    violations: tuple[str, ...]
    state_snapshot: dict[str, Any]


ACTION_MAP = {
    "pg_insert":        lambda sm, e: pg_insert(sm.state, e["record"]),
    "debezium_publish": lambda sm, e: debezium_publish(sm.state, e["record"]),
    "flink_consume":    lambda sm, e: flink_consume(sm.state),
    "warehouse_load":   lambda sm, e: warehouse_load(sm.state, e["record"]),
    "reverse_etl":      lambda sm, e: reverse_etl(sm.state, e["record"]),
}


@dataclass
class Monitor:
    max_lag: int = 100
    max_steps_to_delivery: int = 1_000
    machine: StateMachine = field(default_factory=StateMachine)
    alert_sink: AlertSink = field(default_factory=ListAlertSink)
    liveness: EventualDeliveryWatcher = field(default_factory=EventualDeliveryWatcher)
    incidents: list[Incident] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.liveness.max_steps_to_delivery = self.max_steps_to_delivery

    def replay(self, events: list[dict[str, Any]]) -> list[Incident]:
        for e in events:
            self.machine.step()
            action_name = e["action"]
            fn = ACTION_MAP.get(action_name)
            if fn is None:
                continue
            fn(self.machine, e)
            # Check invariants regardless of whether the action applied
            result = check_all(self.machine.state, max_lag=self.max_lag)
            live_violations = self.liveness.observe(
                self.machine.state, self.machine.step_count
            )
            all_violations = tuple(result.violations) + tuple(live_violations)
            if all_violations:
                incident = Incident(
                    step=self.machine.step_count,
                    action=action_name,
                    violations=all_violations,
                    state_snapshot=self.machine.snapshot(),
                )
                self.incidents.append(incident)
                self.alert_sink.emit({
                    "step": incident.step,
                    "action": incident.action,
                    "violations": list(incident.violations),
                })
        return self.incidents


__all__ = ["Incident", "Monitor"]
