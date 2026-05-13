"""Safety invariants — must hold at EVERY state.

  WarehouseSubsetOfPg   : warehouse ⊆ pg
  RevETLSubsetOfWh      : rev_etl ⊆ warehouse
  KafkaSubsetOfPg       : every record in kafka was inserted in pg
  ExactlyOnceInAgg      : flink_sum[g] equals sum of consumed records with group g
  BoundedLag            : len(kafka) ≤ max_lag
  MonotoneRevETLOffset  : rev_etl never shrinks (caller tracks across steps)
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tlavp.state.machine import State


@dataclass(frozen=True, slots=True)
class SafetyResult:
    ok: bool
    violations: tuple[str, ...]


def warehouse_subset_of_pg(state: State) -> bool:
    return state.warehouse.issubset(state.pg)


def revetl_subset_of_warehouse(state: State) -> bool:
    return state.rev_etl.issubset(state.warehouse)


def kafka_subset_of_pg(state: State) -> bool:
    return all(r in state.pg for r in state.kafka)


def exactly_once_in_agg(state: State) -> bool:
    """flink_sum[g] should equal Σ value for records in `kafka_consumed` with group=g."""
    expected: dict[object, int] = defaultdict(int)
    for _id, group, value in state.kafka_consumed:
        expected[group] += int(value)
    for g, total in state.flink_sum.items():
        if expected.get(g, 0) != total:
            return False
    # Also: flink_sum shouldn't have spurious keys
    return all(g in expected for g in state.flink_sum)


def bounded_lag(state: State, max_lag: int) -> bool:
    return len(state.kafka) <= max_lag


def check_all(state: State, max_lag: int = 1000) -> SafetyResult:
    violations: list[str] = []
    if not warehouse_subset_of_pg(state):
        violations.append("WarehouseSubsetOfPg")
    if not revetl_subset_of_warehouse(state):
        violations.append("RevETLSubsetOfWarehouse")
    if not kafka_subset_of_pg(state):
        violations.append("KafkaSubsetOfPg")
    if not exactly_once_in_agg(state):
        violations.append("ExactlyOnceInAgg")
    if not bounded_lag(state, max_lag):
        violations.append(f"BoundedLag(>{max_lag})")
    return SafetyResult(ok=not violations, violations=tuple(violations))


__all__ = [
    "SafetyResult",
    "bounded_lag",
    "check_all",
    "exactly_once_in_agg",
    "kafka_subset_of_pg",
    "revetl_subset_of_warehouse",
    "warehouse_subset_of_pg",
]
