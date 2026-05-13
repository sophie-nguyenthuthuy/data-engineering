"""Workload generators."""

from __future__ import annotations

import pytest

from tlavp.workload.generator import buggy_stream, healthy_stream


def test_healthy_stream_count():
    events = healthy_stream(n_records=5)
    # 5 stages × 5 records = 25
    assert len(events) == 25


def test_healthy_stream_action_order():
    events = healthy_stream(n_records=1)
    actions = [e["action"] for e in events]
    assert actions == [
        "pg_insert", "debezium_publish", "flink_consume",
        "warehouse_load", "reverse_etl",
    ]


def test_buggy_kafka_lag_only_insert_publish():
    events = buggy_stream("kafka_lag", n_records=3)
    actions = [e["action"] for e in events]
    assert "flink_consume" not in actions


def test_buggy_lost_publish_only_pg_inserts():
    events = buggy_stream("lost_publish", n_records=2)
    actions = [e["action"] for e in events]
    assert all(a == "pg_insert" for a in actions)


def test_buggy_unknown_raises():
    with pytest.raises(ValueError):
        buggy_stream("nonexistent")
