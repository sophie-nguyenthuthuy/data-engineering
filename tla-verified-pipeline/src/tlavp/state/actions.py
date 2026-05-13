"""Action functions — each mirrors a TLA+ Next disjunct.

Returns True if the action could be applied (preconditions satisfied), else
False. The state machine should advance only on True.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tlavp.state.machine import Record, State


def pg_insert(state: State, record: Record) -> bool:
    """PG insert: record must not already be in pg."""
    if record in state.pg:
        return False
    state.pg.add(record)
    return True


def debezium_publish(state: State, record: Record) -> bool:
    """Publish from PG to Kafka. Record must exist in pg; not already in kafka."""
    if record not in state.pg:
        return False
    if record in state.kafka:
        return False
    state.kafka.append(record)
    return True


def flink_consume(state: State) -> bool:
    """Pop head of kafka; update flink_sum for that record's group."""
    if not state.kafka:
        return False
    record = state.kafka.pop(0)
    state.kafka_consumed.append(record)
    _, group, value = record
    state.flink_sum[group] = state.flink_sum.get(group, 0) + int(value)
    return True


def warehouse_load(state: State, record: Record) -> bool:
    """Load `record` into warehouse. Pre: record was consumed by Flink and
    is not already in warehouse."""
    if record in state.warehouse:
        return False
    if record not in state.kafka_consumed:
        return False
    state.warehouse.add(record)
    return True


def reverse_etl(state: State, record: Record) -> bool:
    """Push `record` to reverse-ETL target. Pre: record is in warehouse and
    not already pushed."""
    if record in state.rev_etl:
        return False
    if record not in state.warehouse:
        return False
    state.rev_etl.add(record)
    return True


__all__ = [
    "debezium_publish",
    "flink_consume",
    "pg_insert",
    "reverse_etl",
    "warehouse_load",
]
