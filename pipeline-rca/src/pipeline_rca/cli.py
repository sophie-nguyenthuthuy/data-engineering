"""CLI entry point for pipeline-rca."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import click
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from pipeline_rca.attribution.root_cause import RootCauseAttributor
from pipeline_rca.lineage.tracer import LineageTracer
from pipeline_rca.models import DegradationKind, MetricDegradation, MetricPoint
from pipeline_rca.monitors.metric_monitor import MetricMonitor, build_synthetic_degradation
from pipeline_rca.monitors.schema_monitor import SchemaStore
from pipeline_rca.reporting.generator import ReportGenerator

console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        level=level,
    )


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """Automated root cause attribution for data pipeline failures."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


# ------------------------------------------------------------------
# demo subcommand
# ------------------------------------------------------------------

@cli.command()
@click.option(
    "--drop-pct", default=0.30, show_default=True, help="Simulated metric drop (0-1)."
)
@click.option(
    "--baseline-days", default=14, show_default=True, help="Days in the baseline window."
)
@click.option("--output-dir", default="reports", show_default=True)
@click.option("--save", is_flag=True, help="Write the report to disk.")
def demo(drop_pct: float, baseline_days: int, output_dir: str, save: bool) -> None:
    """Run a self-contained demo with synthetic data (no warehouse required)."""
    console.rule("[bold cyan]pipeline-rca demo[/bold cyan]")

    # 1. Synthetic metric series with a known 30% drop
    series = build_synthetic_degradation(
        baseline_days=baseline_days,
        eval_days=3,
        drop_pct=drop_pct,
    )
    console.print(f"[green]✓[/green] Generated {len(series)}-point synthetic time series")

    # 2. Detect degradation
    monitor = MetricMonitor(
        metric_name="daily_active_users",
        degradation_threshold=0.10,
        baseline_window_days=baseline_days,
        evaluation_window_days=3,
    )
    degradation = monitor.check(series)
    if degradation is None:
        console.print("[yellow]No degradation detected — try increasing --drop-pct[/yellow]")
        sys.exit(0)

    console.print(
        f"[green]✓[/green] Degradation detected: "
        f"[bold red]{degradation.kind.value}[/bold red] "
        f"{abs(degradation.relative_change) * 100:.1f}%"
    )

    # 3. Seed schema store with a synthetic schema change
    store = SchemaStore(":memory:")
    from pipeline_rca.models import ChangeCategoryKind, SchemaChange

    intervention_time = series[baseline_days].timestamp - timedelta(hours=6)
    store.record_pipeline_event(
        table_name="user_events",
        kind=ChangeCategoryKind.COLUMN_DROPPED,
        details={"column": "session_id", "reason": "demo migration"},
        occurred_at=intervention_time,
    )
    # Also register as a schema change
    store._conn.execute(
        "INSERT INTO schema_change_log (table_name, column_name, kind, details, occurred_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            "user_events",
            "session_id",
            ChangeCategoryKind.COLUMN_DROPPED.value,
            json.dumps({"old": {"name": "session_id", "type": "STRING"}}),
            intervention_time.isoformat(),
        ),
    )
    store._conn.commit()
    console.print(
        f"[green]✓[/green] Seeded schema change: "
        f"[bold]user_events.session_id[/bold] dropped at "
        f"{intervention_time.strftime('%Y-%m-%d %H:%M')}"
    )

    # 4. Lineage
    tracer = LineageTracer()
    tracer.register_metric("daily_active_users", ["user_events", "sessions"])
    tracer.register_table_columns("user_events", ["user_id", "session_id", "event_type"])

    # 5. RCA
    attributor = RootCauseAttributor(
        tracer=tracer,
        schema_store=store,
        look_back_days=7,
        confidence_level=0.95,
        min_effect_size=0.03,
    )
    report = attributor.attribute(degradation)
    console.print(f"[green]✓[/green] RCA complete — incident ID: [bold]{report.incident_id}[/bold]")

    # 6. Display summary table
    _display_report_summary(report)

    # 7. Render & optionally save
    gen = ReportGenerator(output_dir=output_dir)
    md = gen.render_markdown(report)

    if save:
        path = gen.save(report)
        console.print(f"\n[green]Report written to:[/green] {path}")
    else:
        console.print("\n[dim]Pass --save to write the report to disk.[/dim]")
        console.print("\n" + md[:2000] + ("\n…[truncated]" if len(md) > 2000 else ""))


# ------------------------------------------------------------------
# run subcommand (config-driven)
# ------------------------------------------------------------------

@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--metric", "-m", multiple=True, help="Metric(s) to evaluate (default: all).")
@click.option("--output-dir", default="reports", show_default=True)
@click.option("--save/--no-save", default=True, show_default=True)
def run(config_path: str, metric: tuple[str, ...], output_dir: str, save: bool) -> None:
    """Run RCA using a YAML config file and metric series loaded from JSON files."""
    with open(config_path) as f:
        config = yaml.safe_load(f)

    tracer = LineageTracer()
    tracer.from_config(config)

    store = SchemaStore(":memory:")

    attributor = RootCauseAttributor(
        tracer=tracer,
        schema_store=store,
        look_back_days=config.get("causal_analysis", {}).get("pre_period_days", 14),
        confidence_level=config.get("causal_analysis", {}).get("confidence_level", 0.95),
        min_effect_size=config.get("causal_analysis", {}).get("min_effect_size", 0.05),
    )

    gen = ReportGenerator(output_dir=output_dir)
    metrics_cfg = config.get("metrics", [])
    target_metrics = set(metric) if metric else {m["name"] for m in metrics_cfg}

    for m_cfg in metrics_cfg:
        if m_cfg["name"] not in target_metrics:
            continue

        series_file = Path(m_cfg.get("series_file", f"{m_cfg['name']}_series.json"))
        if not series_file.exists():
            console.print(f"[yellow]Skipping {m_cfg['name']}: series file {series_file} not found[/yellow]")
            continue

        raw = json.loads(series_file.read_text())
        series = [
            MetricPoint(timestamp=datetime.fromisoformat(p["timestamp"]), value=float(p["value"]))
            for p in raw
        ]

        monitor = MetricMonitor(
            metric_name=m_cfg["name"],
            degradation_threshold=m_cfg.get("degradation_threshold", 0.15),
            baseline_window_days=m_cfg.get("baseline_window_days", 14),
        )
        degradation = monitor.check(series)
        if degradation is None:
            console.print(f"[dim]{m_cfg['name']}: no degradation detected[/dim]")
            continue

        report = attributor.attribute(degradation)
        _display_report_summary(report)

        if save:
            path = gen.save(report)
            console.print(f"[green]Report → {path}[/green]")


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------

def _display_report_summary(report: "RootCauseReport") -> None:  # noqa: F821
    from pipeline_rca.models import RootCauseReport

    pct = abs(report.degradation.relative_change * 100)
    panel = Panel(
        f"[bold]{report.degradation.metric_name}[/bold]  "
        f"[red]{report.degradation.kind.value.upper()} {pct:.1f}%[/red]\n"
        f"baseline={report.degradation.baseline_value:.2f}  "
        f"observed={report.degradation.observed_value:.2f}",
        title=f"[bold]Incident {report.incident_id}[/bold]",
        border_style="red",
    )
    console.print(panel)

    if not report.top_causes:
        console.print("[yellow]No significant root causes found.[/yellow]")
        return

    table = Table(title="Top Root Causes", show_header=True, header_style="bold magenta")
    table.add_column("#", width=3)
    table.add_column("Candidate")
    table.add_column("Effect", justify="right")
    table.add_column("p-value", justify="right")
    table.add_column("Sig?", justify="center")

    for i, cause in enumerate(report.top_causes, 1):
        p_str = f"{cause.p_value:.4f}" if cause.p_value == cause.p_value else "N/A"
        sig_str = "[green]✓[/green]" if cause.is_significant else "[dim]—[/dim]"
        table.add_row(
            str(i),
            cause.candidate,
            f"{cause.effect_size * 100:.1f}%",
            p_str,
            sig_str,
        )

    console.print(table)
