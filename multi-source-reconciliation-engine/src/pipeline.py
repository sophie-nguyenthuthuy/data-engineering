"""Main reconciliation pipeline — orchestrates all stages within the SLA."""
from __future__ import annotations

import time
import uuid
from pathlib import Path

import yaml
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from .classification import DiscrepancyClassifier
from .ingestion import SourceLoader
from .matching import MatchingEngine
from .reporting import ReportGenerator

console = Console()


def load_config(config_path: Path = Path("config/settings.yaml")) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def run_pipeline(
    source_paths: dict[str, str | Path],
    config_path: Path = Path("config/settings.yaml"),
    run_id: str | None = None,
) -> dict:
    """
    Execute the full reconciliation pipeline.

    Parameters
    ----------
    source_paths : dict mapping source name -> file path
    config_path  : path to settings.yaml
    run_id       : optional idempotency key (auto-generated if None)

    Returns
    -------
    dict with keys: run_id, outputs (file paths), summary, sla_met
    """
    cfg = load_config(config_path)
    sla_s = cfg["reconciliation"]["sla_minutes"] * 60
    run_id = run_id or f"RECON-{uuid.uuid4().hex[:8].upper()}"
    t0 = time.perf_counter()

    console.rule(f"[bold blue]Reconciliation Run: {run_id}")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:

        # ── Stage 1: Ingestion ────────────────────────────────────────────────
        task = progress.add_task("[cyan]Ingesting sources…", total=None)
        loader = SourceLoader(cfg)
        sources = loader.load_all({k: Path(v) for k, v in source_paths.items()})
        total_txns = sum(len(v) for v in sources.values())
        progress.update(task, description=f"[green]Ingested {total_txns} transactions")
        _check_sla(t0, sla_s, "ingestion")

        # ── Stage 2: Matching ─────────────────────────────────────────────────
        task = progress.add_task("[cyan]Running fuzzy multi-key matching…", total=None)
        matcher = MatchingEngine(cfg)
        groups = matcher.match(sources)
        progress.update(task, description=f"[green]Formed {len(groups)} match groups")
        _check_sla(t0, sla_s, "matching")

        # ── Stage 3: Classification ───────────────────────────────────────────
        task = progress.add_task("[cyan]Classifying discrepancies…", total=None)
        classifier = DiscrepancyClassifier(cfg)
        discrepancies = classifier.classify(groups)
        progress.update(task, description=f"[green]Found {len(discrepancies)} discrepancies")
        _check_sla(t0, sla_s, "classification")

        # ── Stage 4: Reporting ────────────────────────────────────────────────
        task = progress.add_task("[cyan]Generating reports…", total=None)
        reporter = ReportGenerator(cfg)
        elapsed = time.perf_counter() - t0
        outputs = reporter.generate(run_id, groups, discrepancies, elapsed)
        progress.update(task, description="[green]Reports written")

    elapsed = time.perf_counter() - t0
    sla_met = elapsed <= sla_s

    # ── Console summary ───────────────────────────────────────────────────────
    _print_summary(run_id, sources, groups, discrepancies, elapsed, sla_s, sla_met, outputs)

    summary = {
        "total_transactions": total_txns,
        "total_groups": len(groups),
        "discrepancy_count": len(discrepancies),
    }
    return {
        "run_id": run_id,
        "outputs": {fmt: str(p) for fmt, p in outputs.items()},
        "summary": summary,
        "sla_met": sla_met,
        "elapsed_seconds": round(elapsed, 2),
    }


def _check_sla(t0: float, sla_s: float, stage: str) -> None:
    elapsed = time.perf_counter() - t0
    if elapsed > sla_s:
        console.print(f"[bold red]SLA BREACH at stage={stage}: {elapsed:.1f}s > {sla_s}s")


def _print_summary(run_id, sources, groups, discrepancies, elapsed, sla_s, sla_met, outputs):
    t = Table(title=f"Summary — {run_id}", show_header=True, header_style="bold magenta")
    t.add_column("Metric", style="cyan")
    t.add_column("Value", justify="right")

    for src, txns in sources.items():
        t.add_row(f"  {src}", str(len(txns)))

    t.add_row("Match groups", str(len(groups)))
    clean = len(groups) - len(discrepancies)
    t.add_row("Clean groups", f"[green]{clean}[/green]")
    t.add_row("Discrepancies", f"[yellow]{len(discrepancies)}[/yellow]")

    from collections import Counter
    sev = Counter(d.severity for d in discrepancies)
    for s, c in sorted(sev.items()):
        color = {"LOW": "green", "MEDIUM": "yellow", "HIGH": "orange3", "CRITICAL": "red"}.get(s, "white")
        t.add_row(f"  {s}", f"[{color}]{c}[/{color}]")

    sla_color = "green" if sla_met else "red"
    t.add_row("Elapsed", f"[{sla_color}]{elapsed:.2f}s / {sla_s}s SLA[/{sla_color}]")

    console.print(t)
    for fmt, path in outputs.items():
        console.print(f"  [dim]Report ({fmt}):[/dim] {path}")
