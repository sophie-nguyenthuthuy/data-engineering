"""Tests for adaptive recompilation monitor."""
import json
from pathlib import Path
import pytest

from cle.plan.parser import parse_explain_json
from cle.adaptive.monitor import CardinalityMonitor, CRITICAL_THRESHOLD

FIXTURE = Path(__file__).parent / "fixtures" / "sample_plan.json"


@pytest.fixture
def plan():
    data = json.loads(FIXTURE.read_text())
    return parse_explain_json(data)


def test_monitor_detects_critical_error(plan):
    monitor = CardinalityMonitor(threshold=100.0)
    report = monitor.analyze(plan)
    assert report.needs_replan
    assert report.affected_nodes >= 1
    assert report.worst_q_error > 100


def test_monitor_high_threshold(plan):
    monitor = CardinalityMonitor(threshold=10_000.0)
    report = monitor.analyze(plan)
    assert not report.needs_replan
    assert report.affected_nodes == 0


def test_correction_map(plan):
    monitor = CardinalityMonitor(threshold=100.0)
    report = monitor.analyze(plan)
    cmap = monitor.correction_map(report)
    assert isinstance(cmap, dict)
    # Should contain the root node (Hash Join) with its actual rows
    assert len(cmap) >= 1
    for node_id, rows in cmap.items():
        assert isinstance(node_id, int)
        assert rows >= 0


def test_alert_direction(plan):
    monitor = CardinalityMonitor(threshold=10.0)
    report = monitor.analyze(plan)
    for alert in report.alerts:
        assert alert.direction in ("over", "under")
        if alert.direction == "over":
            assert alert.estimated > alert.actual
        else:
            assert alert.actual > alert.estimated


def test_summary_string(plan):
    monitor = CardinalityMonitor(threshold=100.0)
    report = monitor.analyze(plan)
    summary = report.summary()
    assert "REPLAN" in summary or "OK" in summary


def test_monitor_no_actuals():
    """Monitor should handle plans without actuals gracefully."""
    import json
    # Plan without actual rows
    raw = [{"Plan": {
        "Node Type": "Seq Scan",
        "Plan Rows": 1000,
        "Plan Width": 8,
        "Startup Cost": 0.0,
        "Total Cost": 50.0,
    }}]
    from cle.plan.parser import parse_explain_json
    plan = parse_explain_json(raw)
    monitor = CardinalityMonitor()
    report = monitor.analyze(plan)
    assert not report.needs_replan
    assert report.total_nodes == 0
