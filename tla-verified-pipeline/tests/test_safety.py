"""Safety invariants."""

from __future__ import annotations

from tlavp.invariants.safety import (
    bounded_lag,
    check_all,
    exactly_once_in_agg,
    kafka_subset_of_pg,
    revetl_subset_of_warehouse,
    warehouse_subset_of_pg,
)
from tlavp.state.actions import debezium_publish, flink_consume, pg_insert, warehouse_load
from tlavp.state.machine import State


def test_clean_state_all_invariants_hold():
    s = State()
    pg_insert(s, (1, "A", 10))
    debezium_publish(s, (1, "A", 10))
    flink_consume(s)
    warehouse_load(s, (1, "A", 10))
    r = check_all(s, max_lag=10)
    assert r.ok
    assert r.violations == ()


def test_warehouse_subset_of_pg_caught():
    s = State()
    # Direct corruption: a record in warehouse that's not in PG
    s.warehouse.add((99, "Z", 0))
    assert not warehouse_subset_of_pg(s)
    r = check_all(s)
    assert "WarehouseSubsetOfPg" in r.violations


def test_revetl_subset_of_warehouse_caught():
    s = State()
    s.rev_etl.add((99, "Z", 0))
    assert not revetl_subset_of_warehouse(s)


def test_kafka_subset_of_pg_caught():
    s = State()
    s.kafka.append((99, "Z", 0))    # not in pg
    assert not kafka_subset_of_pg(s)


def test_exactly_once_in_agg_caught():
    """If we tamper with flink_sum without matching consumed records, it should fail."""
    s = State()
    s.kafka_consumed.append((1, "A", 10))
    s.flink_sum["A"] = 999    # wrong
    assert not exactly_once_in_agg(s)


def test_bounded_lag_caught():
    s = State()
    for i in range(150):
        s.kafka.append((i, "A", 1))
    assert not bounded_lag(s, max_lag=100)


def test_clean_pipeline_check_all_passes():
    s = State()
    for i in range(5):
        r = (i, "A", i + 1)
        pg_insert(s, r)
        debezium_publish(s, r)
        flink_consume(s)
        warehouse_load(s, r)
    result = check_all(s, max_lag=10)
    assert result.ok, f"unexpected violations: {result.violations}"
