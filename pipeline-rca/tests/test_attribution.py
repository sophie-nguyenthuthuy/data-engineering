"""Integration-style tests for the full attribution pipeline."""

import json
from datetime import datetime, timedelta

from pipeline_rca.attribution.root_cause import RootCauseAttributor
from pipeline_rca.lineage.tracer import LineageTracer
from pipeline_rca.models import ChangeCategoryKind, DegradationKind, MetricDegradation, MetricPoint
from pipeline_rca.monitors.metric_monitor import build_synthetic_degradation
from pipeline_rca.monitors.schema_monitor import SchemaStore


def _make_degradation(relative_change: float = -0.35) -> MetricDegradation:
    series = build_synthetic_degradation(baseline_days=14, eval_days=3, drop_pct=0.35)
    return MetricDegradation(
        metric_name="daily_active_users",
        detected_at=series[-1].timestamp,
        kind=DegradationKind.DROP,
        observed_value=650.0,
        baseline_value=1000.0,
        relative_change=relative_change,
        series=series,
    )


class TestRootCauseAttributor:
    def setup_method(self):
        self.store = SchemaStore(":memory:")
        self.tracer = LineageTracer()
        self.tracer.register_metric("daily_active_users", ["user_events", "sessions"])

    def _make_attributor(self, **kwargs):
        return RootCauseAttributor(
            tracer=self.tracer,
            schema_store=self.store,
            look_back_days=7,
            min_effect_size=0.03,
            **kwargs,
        )

    def test_produces_report_with_no_changes(self):
        degradation = _make_degradation()
        attributor = self._make_attributor()
        report = attributor.attribute(degradation)
        assert report.incident_id
        assert len(report.top_causes) >= 1
        # catch-all unknown candidate
        assert "unknown" in report.top_causes[0].candidate.lower()

    def test_produces_report_with_schema_change(self):
        degradation = _make_degradation()
        intervention = degradation.series[14].timestamp - timedelta(hours=3)
        self.store._conn.execute(
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
        self.store._conn.commit()

        attributor = self._make_attributor()
        report = attributor.attribute(degradation)
        assert report.incident_id
        assert any("user_events" in c.candidate for c in report.all_candidates)

    def test_incident_id_is_unique(self):
        degradation = _make_degradation()
        attributor = self._make_attributor()
        r1 = attributor.attribute(degradation)
        r2 = attributor.attribute(degradation)
        assert r1.incident_id != r2.incident_id
