"""Tests for the incident report generator."""

import json
from datetime import datetime, timedelta

from pipeline_rca.attribution.root_cause import RootCauseAttributor
from pipeline_rca.lineage.tracer import LineageTracer
from pipeline_rca.models import ChangeCategoryKind, DegradationKind, MetricDegradation
from pipeline_rca.monitors.metric_monitor import build_synthetic_degradation
from pipeline_rca.monitors.schema_monitor import SchemaStore
from pipeline_rca.reporting.generator import ReportGenerator


def _full_report():
    series = build_synthetic_degradation(baseline_days=14, eval_days=3, drop_pct=0.35)
    degradation = MetricDegradation(
        metric_name="daily_active_users",
        detected_at=series[-1].timestamp,
        kind=DegradationKind.DROP,
        observed_value=650.0,
        baseline_value=1000.0,
        relative_change=-0.35,
        series=series,
    )
    store = SchemaStore(":memory:")
    intervention = series[14].timestamp - timedelta(hours=3)
    store._conn.execute(
        "INSERT INTO schema_change_log (table_name, column_name, kind, details, occurred_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            "user_events",
            "session_id",
            ChangeCategoryKind.COLUMN_DROPPED.value,
            json.dumps({"old": {"name": "session_id", "type": "STRING"}}),
            intervention.isoformat(),
        ),
    )
    store._conn.commit()
    tracer = LineageTracer()
    tracer.register_metric("daily_active_users", ["user_events"])
    attributor = RootCauseAttributor(tracer=tracer, schema_store=store, min_effect_size=0.01)
    return attributor.attribute(degradation)


class TestReportGenerator:
    def test_renders_markdown(self):
        report = _full_report()
        gen = ReportGenerator()
        md = gen.render_markdown(report)
        assert "# Incident Report" in md
        assert report.incident_id in md
        assert "daily_active_users" in md

    def test_markdown_contains_top_causes(self):
        report = _full_report()
        gen = ReportGenerator()
        md = gen.render_markdown(report)
        assert "Top Root Causes" in md

    def test_save_writes_file(self, tmp_path):
        report = _full_report()
        gen = ReportGenerator(output_dir=tmp_path)
        path = gen.save(report)
        assert path.exists()
        content = path.read_text()
        assert report.incident_id in content
