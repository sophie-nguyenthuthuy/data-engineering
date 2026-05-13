"""State machine actions."""

from __future__ import annotations

from tlavp.state.actions import (
    debezium_publish,
    flink_consume,
    pg_insert,
    reverse_etl,
    warehouse_load,
)
from tlavp.state.machine import State


def test_pg_insert_adds_record():
    s = State()
    assert pg_insert(s, (1, "A", 10))
    assert (1, "A", 10) in s.pg


def test_pg_insert_idempotent():
    s = State()
    pg_insert(s, (1, "A", 10))
    assert not pg_insert(s, (1, "A", 10))


def test_debezium_publish_requires_pg():
    s = State()
    assert not debezium_publish(s, (1, "A", 10))
    pg_insert(s, (1, "A", 10))
    assert debezium_publish(s, (1, "A", 10))
    assert (1, "A", 10) in s.kafka


def test_debezium_publish_idempotent():
    s = State()
    pg_insert(s, (1, "A", 10))
    debezium_publish(s, (1, "A", 10))
    assert not debezium_publish(s, (1, "A", 10))


def test_flink_consume_pops_head():
    s = State()
    pg_insert(s, (1, "A", 10))
    debezium_publish(s, (1, "A", 10))
    assert flink_consume(s)
    assert s.kafka == []
    assert s.flink_sum["A"] == 10


def test_flink_consume_empty():
    s = State()
    assert not flink_consume(s)


def test_warehouse_load_requires_consume():
    s = State()
    pg_insert(s, (1, "A", 10))
    debezium_publish(s, (1, "A", 10))
    # Before consume — load fails
    assert not warehouse_load(s, (1, "A", 10))
    flink_consume(s)
    assert warehouse_load(s, (1, "A", 10))


def test_reverse_etl_requires_warehouse():
    s = State()
    pg_insert(s, (1, "A", 10))
    debezium_publish(s, (1, "A", 10))
    flink_consume(s)
    assert not reverse_etl(s, (1, "A", 10))
    warehouse_load(s, (1, "A", 10))
    assert reverse_etl(s, (1, "A", 10))
