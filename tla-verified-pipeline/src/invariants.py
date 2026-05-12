"""Invariants matching TLA+ properties."""
from __future__ import annotations

from dataclasses import dataclass

from .state_machine import State


class InvariantViolation(Exception):
    pass


def exactly_once_in_agg(s: State) -> bool:
    """flink_sum = |records consumed by Flink| = |pg| - |kafka unconsumed|.

    More precisely: consumed = flink_sum; pending = |kafka|;
    records that have entered the pipeline = consumed + pending.
    The records that have entered must be a subset of pg.
    """
    consumed = s.flink_sum
    pending = len(s.kafka)
    return consumed + pending <= len(s.pg)


def warehouse_subset_of_pg(s: State) -> bool:
    """No phantom records: nothing in DW that's not in PG."""
    return s.warehouse.issubset(s.pg)


def reverse_etl_subset_of_warehouse(s: State) -> bool:
    return s.rev_etl.issubset(s.warehouse)


def bounded_lag(s: State, max_lag: int) -> bool:
    return len(s.kafka) <= max_lag


def check_all(s: State, max_lag: int = 1000) -> list[str]:
    """Return list of violated invariant names. Empty = all ok."""
    violations = []
    if not exactly_once_in_agg(s):
        violations.append("ExactlyOnceInAgg")
    if not warehouse_subset_of_pg(s):
        violations.append("WarehouseSubsetOfPg")
    if not reverse_etl_subset_of_warehouse(s):
        violations.append("ReverseEtlSubsetOfWarehouse")
    if not bounded_lag(s, max_lag):
        violations.append("BoundedLag")
    return violations


__all__ = ["InvariantViolation", "exactly_once_in_agg", "warehouse_subset_of_pg",
           "reverse_etl_subset_of_warehouse", "bounded_lag", "check_all"]
