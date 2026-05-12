"""Command-line interface for the replay engine."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich import box

from .event import EventLog
from .replay import ReplayEngine

console = Console()


@click.group()
def main() -> None:
    """Deterministic Replay Engine — replay distributed event logs causally."""


@main.command("replay")
@click.argument("log_file", type=click.Path(exists=True, dir_okay=False))
@click.option("--format", "fmt", default="auto", type=click.Choice(["auto", "json", "jsonl"]))
@click.option("--udf-runs", default=2, show_default=True, help="Runs per event for UDF checks.")
@click.option("--stop-on-violation", is_flag=True, help="Abort on first violation.")
@click.option("--output", "-o", type=click.Path(), help="Write ordered log to file (JSONL).")
@click.option("--report", "-r", type=click.Path(), help="Write JSON report to file.")
def replay_cmd(
    log_file: str,
    fmt: str,
    udf_runs: int,
    stop_on_violation: bool,
    output: str | None,
    report: str | None,
) -> None:
    """Replay LOG_FILE deterministically, detecting causal and exactly-once violations."""
    path = Path(log_file)
    text = path.read_text()

    if fmt == "auto":
        fmt = "jsonl" if path.suffix in (".jsonl", ".ndjson") else "json"

    if fmt == "jsonl":
        log = EventLog.from_jsonl(text)
    else:
        log = EventLog.from_json(text)

    console.print(f"[bold]Loaded[/bold] {len(log)} events from {path.name}")

    engine = ReplayEngine(udf_runs=udf_runs, stop_on_violation=stop_on_violation)
    result = engine.replay(log)

    # Ordered events table
    table = Table(title="Replayed Events (causal order)", box=box.SIMPLE_HEAD)
    table.add_column("Pos", style="dim", width=4)
    table.add_column("Event ID")
    table.add_column("Producer")
    table.add_column("Seq", justify="right")
    table.add_column("Vector Clock")
    table.add_column("Violations")

    for pos, step in enumerate(result.steps):
        e = step.event
        violations_str = ""
        if step.exactly_once_violations:
            kinds = ", ".join(v.kind.name for v in step.exactly_once_violations)
            violations_str = f"[red]{kinds}[/red]"
        table.add_row(
            str(pos),
            e.event_id,
            e.producer_id,
            str(e.sequence_num),
            str(e.vector_clock),
            violations_str or "[green]OK[/green]",
        )

    console.print(table)

    # Summary
    console.rule("Summary")
    console.print(result.summary())

    if result.sequence_errors:
        console.print("\n[yellow]Sequence warnings:[/yellow]")
        for err in result.sequence_errors:
            console.print(f"  {err}")

    # Write ordered log
    if output:
        out_path = Path(output)
        out_path.write_text(
            "\n".join(json.dumps(e.to_dict()) for e in result.ordered_events)
        )
        console.print(f"\nOrdered log written to [cyan]{out_path}[/cyan]")

    # Write report
    if report:
        report_data = {
            "summary": result.summary(),
            "success": result.success,
            "duration_ms": result.duration_ms,
            "exactly_once": result.exactly_once_report,
            "udfs": result.udf_reports,
            "sequence_errors": result.sequence_errors,
        }
        Path(report).write_text(json.dumps(report_data, indent=2))
        console.print(f"Report written to [cyan]{report}[/cyan]")

    sys.exit(0 if result.success else 1)


@main.command("validate")
@click.argument("log_file", type=click.Path(exists=True, dir_okay=False))
@click.option("--format", "fmt", default="auto", type=click.Choice(["auto", "json", "jsonl"]))
def validate_cmd(log_file: str, fmt: str) -> None:
    """Validate the causal structure of LOG_FILE without replaying."""
    from .causal_order import validate_monotone_sequences, causal_sort

    path = Path(log_file)
    text = path.read_text()
    if fmt == "auto":
        fmt = "jsonl" if path.suffix in (".jsonl", ".ndjson") else "json"
    log = EventLog.from_jsonl(text) if fmt == "jsonl" else EventLog.from_json(text)

    errors = validate_monotone_sequences(list(log))
    if errors:
        console.print("[red]Sequence errors:[/red]")
        for e in errors:
            console.print(f"  {e}")
    else:
        console.print("[green]All producer sequences are monotone.[/green]")

    try:
        ordered = causal_sort(list(log))
        console.print(f"[green]Causal sort OK:[/green] {len(ordered)} events")
    except Exception as exc:
        console.print(f"[red]Causal sort failed:[/red] {exc}")
        sys.exit(1)
