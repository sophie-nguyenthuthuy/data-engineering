"""
Standalone demo — runs the full RCA pipeline with synthetic data.
No warehouse connection required.

Usage:
    python examples/demo.py
"""

from __future__ import annotations

import json
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline_rca.attribution.root_cause import RootCauseAttributor
from pipeline_rca.lineage.tracer import LineageTracer
from pipeline_rca.models import ChangeCategoryKind
from pipeline_rca.monitors.metric_monitor import MetricMonitor, build_synthetic_degradation
from pipeline_rca.monitors.schema_monitor import SchemaStore
from pipeline_rca.reporting.generator import ReportGenerator


def main() -> None:
    print("=" * 60)
    print("  pipeline-rca  — Automated Root Cause Attribution Demo")
    print("=" * 60)

    # ── 1. Synthetic time series with a 35% drop ──────────────────
    series = build_synthetic_degradation(
        baseline_days=14,
        eval_days=3,
        baseline_mean=10_000,
        drop_pct=0.35,
        noise_pct=0.03,
    )
    print(f"\n[1] Generated {len(series)}-point series for 'daily_active_users'")

    # ── 2. Detect the degradation ─────────────────────────────────
    monitor = MetricMonitor(
        metric_name="daily_active_users",
        degradation_threshold=0.10,
        baseline_window_days=14,
        evaluation_window_days=3,
        z_threshold=2.0,
    )
    degradation = monitor.check(series)
    assert degradation is not None, "Expected degradation not detected — adjust parameters"
    print(
        f"[2] Degradation detected: {degradation.kind.value.upper()} "
        f"{abs(degradation.relative_change) * 100:.1f}%  "
        f"(baseline={degradation.baseline_value:.0f}, "
        f"observed={degradation.observed_value:.0f})"
    )

    # ── 3. Seed schema store with a realistic event ───────────────
    store = SchemaStore(":memory:")
    intervention_time = series[14].timestamp - timedelta(hours=4)

    store._conn.execute(
        "INSERT INTO schema_change_log "
        "(table_name, column_name, kind, details, occurred_at) VALUES (?,?,?,?,?)",
        (
            "user_events",
            "session_id",
            ChangeCategoryKind.COLUMN_DROPPED.value,
            json.dumps({"old": {"name": "session_id", "type": "STRING"}}),
            intervention_time.isoformat(),
        ),
    )
    store._conn.commit()

    store.record_pipeline_event(
        table_name="sessions",
        kind=ChangeCategoryKind.LATE_DATA,
        details={"delay_hours": 6, "partition": str(series[14].timestamp.date())},
        occurred_at=series[14].timestamp - timedelta(hours=2),
    )
    print(
        f"[3] Seeded 2 candidate changes:\n"
        f"    • user_events.session_id COLUMN_DROPPED @ {intervention_time:%Y-%m-%d %H:%M}\n"
        f"    • sessions LATE_DATA @ {series[14].timestamp - timedelta(hours=2):%Y-%m-%d %H:%M}"
    )

    # ── 4. Lineage ────────────────────────────────────────────────
    tracer = LineageTracer()
    tracer.register_metric("daily_active_users", ["user_events", "sessions"])
    tracer.register_table_columns("user_events", ["user_id", "session_id", "event_type", "ts"])
    tracer.register_table_columns("sessions", ["session_id", "user_id", "started_at"])
    print("[4] Registered lineage: daily_active_users → [user_events, sessions]")

    # ── 5. RCA ────────────────────────────────────────────────────
    attributor = RootCauseAttributor(
        tracer=tracer,
        schema_store=store,
        look_back_days=7,
        confidence_level=0.95,
        min_effect_size=0.03,
    )
    report = attributor.attribute(degradation)
    print(f"\n[5] RCA complete — Incident ID: {report.incident_id}")
    print(f"    Evaluated {len(report.all_candidates)} candidate(s)")
    print(f"    Top causes: {len(report.top_causes)}")

    for i, cause in enumerate(report.top_causes, 1):
        sig = "✓ significant" if cause.is_significant else "— not significant"
        p = f"p={cause.p_value:.4f}" if cause.p_value == cause.p_value else "p=N/A"
        print(
            f"    {i}. {cause.candidate}  |  "
            f"effect={cause.effect_size * 100:.1f}%  {p}  [{sig}]"
        )

    # ── 6. Render report ─────────────────────────────────────────
    gen = ReportGenerator(output_dir="reports")
    path = gen.save(report)
    print(f"\n[6] Report written to: {path}")

    with open(path) as f:
        print("\n" + "─" * 60)
        print(f.read()[:3000])
    print("─" * 60)


if __name__ == "__main__":
    main()
