"""Connectors."""

from __future__ import annotations

from tlavp.connectors.dw_changelog import dw_load_events, reverse_etl_events
from tlavp.connectors.kafka_log import flink_consume_events, kafka_publish_events
from tlavp.connectors.pg_wal import pg_insert_events


def test_pg_insert_events_format():
    records = [(1, "A", 10), (2, "B", 20)]
    events = list(pg_insert_events(records))
    assert len(events) == 2
    assert all(e["action"] == "pg_insert" for e in events)
    assert events[0]["record"] == (1, "A", 10)


def test_kafka_publish_events():
    events = list(kafka_publish_events([(1, "A", 10)]))
    assert events == [{"action": "debezium_publish", "record": (1, "A", 10)}]


def test_flink_consume_events_count():
    events = list(flink_consume_events(5))
    assert len(events) == 5
    assert all(e["action"] == "flink_consume" for e in events)


def test_dw_load_events():
    events = list(dw_load_events([(1, "A", 10)]))
    assert events[0]["action"] == "warehouse_load"


def test_reverse_etl_events():
    events = list(reverse_etl_events([(1, "A", 10)]))
    assert events[0]["action"] == "reverse_etl"


def test_pipeline_assembly():
    """Compose connectors to build a complete pipeline event stream."""
    records = [(i, "A", i * 10) for i in range(3)]
    full = (
        list(pg_insert_events(records))
        + list(kafka_publish_events(records))
        + list(flink_consume_events(len(records)))
        + list(dw_load_events(records))
        + list(reverse_etl_events(records))
    )
    actions = [e["action"] for e in full]
    assert actions.count("pg_insert") == 3
    assert actions.count("reverse_etl") == 3
