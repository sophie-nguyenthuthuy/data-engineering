"""End-to-end monitor replay."""

from __future__ import annotations

import pytest

from tlavp.monitor.alerts import ListAlertSink
from tlavp.monitor.replay import Monitor
from tlavp.workload import buggy_stream, healthy_stream


def test_healthy_stream_no_incidents(monitor):
    events = healthy_stream(n_records=5)
    monitor.replay(events)
    assert monitor.incidents == []


def test_clean_run_delivers_all_records():
    monitor = Monitor(max_lag=10, max_steps_to_delivery=100)
    events = healthy_stream(n_records=10)
    monitor.replay(events)
    assert len(monitor.machine.state.rev_etl) == 10
    assert monitor.incidents == []


def test_kafka_lag_caught(monitor):
    events = buggy_stream("kafka_lag", n_records=15)
    monitor.replay(events)
    # max_lag=10 → at some point kafka has > 10 records
    bounded_lag_violations = [
        inc for inc in monitor.incidents
        if any("BoundedLag" in v for v in inc.violations)
    ]
    assert len(bounded_lag_violations) > 0


def test_alert_sink_receives_emissions():
    sink = ListAlertSink()
    monitor = Monitor(max_lag=5, alert_sink=sink)
    events = buggy_stream("kafka_lag", n_records=10)
    monitor.replay(events)
    assert len(sink.incidents) > 0
    # Sink received same number of alerts as monitor recorded
    assert len(sink.incidents) == len(monitor.incidents)


@pytest.mark.bugs
def test_lost_publish_eventually_flags_liveness():
    """pg_insert without publish → EventualDelivery should fire."""
    monitor = Monitor(max_lag=100, max_steps_to_delivery=2)
    events = list(buggy_stream("lost_publish", n_records=3))
    # Pad with no-op consumes to advance the step counter past max_steps_to_delivery
    for _ in range(50):
        events.append({"action": "flink_consume"})
    monitor.replay(events)
    liveness_violations = [
        inc for inc in monitor.incidents
        if any("EventualDelivery" in v for v in inc.violations)
    ]
    assert len(liveness_violations) > 0


def test_state_snapshot_in_incident():
    monitor = Monitor(max_lag=2)
    events = buggy_stream("kafka_lag", n_records=5)
    monitor.replay(events)
    assert monitor.incidents
    inc = monitor.incidents[0]
    snap = inc.state_snapshot
    assert "pg" in snap and "kafka" in snap


def test_monitor_step_counter_advances():
    monitor = Monitor()
    events = healthy_stream(n_records=3)
    monitor.replay(events)
    assert monitor.machine.step_count == len(events)
