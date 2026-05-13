"""Liveness watcher."""

from __future__ import annotations

from tlavp.invariants.liveness import EventualDeliveryWatcher
from tlavp.state.actions import (
    debezium_publish,
    flink_consume,
    pg_insert,
    reverse_etl,
    warehouse_load,
)
from tlavp.state.machine import State


def test_no_violations_on_clean_run():
    s = State()
    w = EventualDeliveryWatcher(max_steps_to_delivery=10)
    r = (1, "A", 10)
    pg_insert(s, r)
    debezium_publish(s, r)
    flink_consume(s)
    warehouse_load(s, r)
    reverse_etl(s, r)
    violations = w.observe(s, step=5)
    assert violations == []


def test_overdue_record_flagged():
    s = State()
    w = EventualDeliveryWatcher(max_steps_to_delivery=5)
    r = (1, "A", 10)
    pg_insert(s, r)
    # Insert at step 1; observe at step 10 (5 steps overdue)
    w.observe(s, step=1)
    violations = w.observe(s, step=10)
    assert any("EventualDelivery" in v for v in violations)


def test_latency_average_after_clean_run():
    s = State()
    w = EventualDeliveryWatcher()
    r = (1, "A", 10)
    pg_insert(s, r)
    w.observe(s, step=1)
    debezium_publish(s, r)
    flink_consume(s)
    warehouse_load(s, r)
    reverse_etl(s, r)
    w.observe(s, step=5)
    assert w.average_latency() == 4   # delivered at step 5 minus inserted at step 1
