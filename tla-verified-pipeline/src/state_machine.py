"""Python mirror of the TLA+ state machine.

State variables and Next actions match `spec/pipeline.tla`. We can drive this
from events (replay mode) and check invariants at every step.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


@dataclass
class State:
    pg: set = field(default_factory=set)
    kafka: deque = field(default_factory=deque)
    flink_sum: int = 0
    warehouse: set = field(default_factory=set)
    rev_etl: set = field(default_factory=set)

    def copy(self) -> "State":
        return State(
            pg=set(self.pg),
            kafka=deque(self.kafka),
            flink_sum=self.flink_sum,
            warehouse=set(self.warehouse),
            rev_etl=set(self.rev_etl),
        )


# ---- Actions (mirror TLA+ Next disjuncts) ---------------------------------

def pg_insert(s: State, r) -> bool:
    if r in s.pg:
        return False
    s.pg.add(r)
    return True


def debezium_publish(s: State, r) -> bool:
    if r not in s.pg:
        return False
    if r in s.kafka:
        return False
    s.kafka.append(r)
    return True


def flink_consume(s: State) -> bool:
    if not s.kafka:
        return False
    s.kafka.popleft()
    s.flink_sum += 1
    return True


def warehouse_load(s: State, r) -> bool:
    if r not in s.pg:
        return False
    if r in s.warehouse:
        return False
    if r in s.kafka:
        return False  # must have been consumed by Flink first
    s.warehouse.add(r)
    return True


def reverse_etl(s: State, r) -> bool:
    if r not in s.warehouse:
        return False
    if r in s.rev_etl:
        return False
    s.rev_etl.add(r)
    return True


__all__ = ["State", "pg_insert", "debezium_publish", "flink_consume",
           "warehouse_load", "reverse_etl"]
