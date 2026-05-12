"""
CLI entry-point.  Install the package (`pip install -e .`) then run:

    sre --help
    sre scrape
    sre scrape --banks VCB,BIDV
    sre serve
    sre rates --term 180
    sre trends VCB --term 365
    sre compare --term 180
"""
import logging
import sys
from datetime import datetime, timedelta
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich import box

from savings_engine.storage.database import init_db

app = typer.Typer(
    name="sre",
    help="Savings Rate Engine — Vietnamese bank rate scraper & analysis tool",
    no_args_is_help=True,
)
console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )


# ── Commands ──────────────────────────────────────────────────────────────────

@app.command()
def scrape(
    banks: Optional[str] = typer.Option(
        None, "--banks", "-b",
        help="Comma-separated bank codes to scrape (default: all). E.g. VCB,BIDV,TCB",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    """Run the scraping pipeline once (all banks or a subset)."""
    _setup_logging(verbose)
    from savings_engine.pipeline import run_pipeline

    bank_list = [b.strip().upper() for b in banks.split(",")] if banks else None
    run = run_pipeline(bank_list)

    table = Table(title="Pipeline Run", box=box.ROUNDED)
    table.add_column("Bank", style="cyan")
    table.add_column("Status")
    table.add_column("Rates", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Error")

    for r in run.results:
        status = "[green]✓[/]" if r.success else "[red]✗[/]"
        table.add_row(r.bank_code, status, str(r.rates_saved),
                      f"{r.duration_s:.2f}s", r.error or "")

    console.print(table)
    console.print(
        f"\n[bold]Total:[/] {run.successful_banks}/{run.total_banks} banks OK, "
        f"{run.total_rates} rates, {run.duration_s:.1f}s"
    )
    raise typer.Exit(0 if run.successful_banks > 0 else 1)


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", "--host"),
    port: int = typer.Option(8000, "--port", "-p"),
    reload: bool = typer.Option(False, "--reload"),
):
    """Start the FastAPI server."""
    _setup_logging(False)
    import uvicorn
    from savings_engine.api.app import create_app  # noqa: F401 — ensure app is importable
    uvicorn.run(
        "savings_engine.api.app:create_app",
        factory=True,
        host=host,
        port=port,
        reload=reload,
    )


@app.command()
def rates(
    bank: Optional[str] = typer.Option(None, "--bank", "-b", help="Bank code, e.g. VCB"),
    term: Optional[int] = typer.Option(None, "--term", "-t", help="Term in days, e.g. 180"),
    rate_type: str = typer.Option("standard", "--type"),
):
    """Show latest rates from the database."""
    _setup_logging(False)
    init_db()
    from savings_engine.storage.database import SessionLocal
    from savings_engine.storage.repository import RateRepository

    with SessionLocal() as db:
        repo = RateRepository(db)
        records = repo.get_latest_rates(bank_code=bank.upper() if bank else None)

    if rate_type:
        records = [r for r in records if r.rate_type == rate_type]
    if term is not None:
        records = [r for r in records if r.term_days == term]

    if not records:
        console.print("[yellow]No rates found. Run [bold]sre scrape[/bold] first.[/]")
        raise typer.Exit(1)

    table = Table(title="Latest Rates", box=box.ROUNDED)
    table.add_column("Bank",       style="cyan")
    table.add_column("Term",       justify="right")
    table.add_column("Label")
    table.add_column("Rate %p.a.", justify="right", style="green")
    table.add_column("Type")

    for r in sorted(records, key=lambda x: (x.bank_code, x.term_days)):
        table.add_row(r.bank_code, str(r.term_days), r.term_label,
                      f"{r.rate_pa:.2f}%", r.rate_type)

    console.print(table)


@app.command()
def compare(
    term: int = typer.Option(..., "--term", "-t", help="Term in days, e.g. 180"),
    rate_type: str = typer.Option("standard", "--type"),
    top_n: int = typer.Option(10, "--top"),
):
    """Rank all banks by rate for a given term."""
    _setup_logging(False)
    init_db()
    from savings_engine.storage.database import SessionLocal
    from savings_engine.storage.repository import RateRepository
    from savings_engine.analyzer.comparisons import compare_banks

    with SessionLocal() as db:
        repo = RateRepository(db)
        comparisons = compare_banks(repo, term, rate_type=rate_type, top_n=top_n)

    if not comparisons:
        console.print(f"[yellow]No data for term={term}d. Run [bold]sre scrape[/bold] first.[/]")
        raise typer.Exit(1)

    table = Table(title=f"Best rates — {term}d {rate_type}", box=box.ROUNDED)
    table.add_column("#",         justify="right")
    table.add_column("Bank",      style="cyan")
    table.add_column("Name")
    table.add_column("Rate %p.a.", justify="right", style="green")

    for c in comparisons:
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(c.rank, str(c.rank))
        table.add_row(medal, c.bank_code, c.bank_name_vi, f"{c.rate_pa:.2f}%")

    console.print(table)


@app.command()
def trends(
    bank_code: str = typer.Argument(..., help="Bank code, e.g. VCB"),
    term: int = typer.Option(..., "--term", "-t", help="Term in days, e.g. 365"),
    rate_type: str = typer.Option("standard", "--type"),
    days_back: int = typer.Option(90, "--days"),
):
    """Show rate trend history for a bank + term."""
    _setup_logging(False)
    init_db()
    from savings_engine.storage.database import SessionLocal
    from savings_engine.storage.repository import RateRepository
    from savings_engine.analyzer.trends import compute_trend

    since = datetime.utcnow() - timedelta(days=days_back)
    with SessionLocal() as db:
        repo = RateRepository(db)
        history = repo.get_rate_history(bank_code.upper(), term, rate_type, since=since)

    summary = compute_trend(history, bank_code.upper(), term, rate_type)
    if not summary:
        console.print(f"[yellow]No history found for {bank_code}/{term}d. Need more scrape runs.[/]")
        raise typer.Exit(1)

    arrow = {"up": "↑ ", "down": "↓ ", "stable": "→ "}.get(summary.direction, "")
    direction_style = {"up": "green", "down": "red", "stable": "yellow"}.get(summary.direction)

    console.print(f"\n[bold]{bank_code.upper()} — {term}d {rate_type}[/]")
    console.print(f"  Current : [bold]{summary.current_rate:.2f}%[/]  [{direction_style}]{arrow}{summary.direction}[/]")
    console.print(f"  Range   : {summary.min_rate:.2f}% — {summary.max_rate:.2f}%  (avg {summary.avg_rate:.2f}%)")
    console.print(f"  Δ  7d   : {_fmt_delta(summary.change_7d)}")
    console.print(f"  Δ 30d   : {_fmt_delta(summary.change_30d)}")
    console.print(f"  Δ 90d   : {_fmt_delta(summary.change_90d)}")

    table = Table(title="History", box=box.SIMPLE)
    table.add_column("Date")
    table.add_column("Rate %p.a.", justify="right")
    table.add_column("Δ pp", justify="right")

    for p in summary.points[-20:]:  # last 20 data points
        delta_str = _fmt_delta(p.delta_from_prev) if p.delta_from_prev is not None else ""
        table.add_row(p.scraped_at.strftime("%Y-%m-%d %H:%M"), f"{p.rate_pa:.2f}%", delta_str)

    console.print(table)


@app.command()
def schedule(
    interval_hours: int = typer.Option(6, "--interval", "-i", help="Scrape interval in hours"),
):
    """Start the background scheduler (blocks until interrupted)."""
    _setup_logging(False)
    from savings_engine.scheduler import start_scheduler
    console.print(f"[green]Starting scheduler — every {interval_hours}h[/]  (Ctrl-C to stop)")
    start_scheduler(interval_hours=interval_hours)


def _fmt_delta(delta: Optional[float]) -> str:
    if delta is None:
        return "[dim]n/a[/]"
    color = "green" if delta > 0 else ("red" if delta < 0 else "white")
    sign = "+" if delta > 0 else ""
    return f"[{color}]{sign}{delta:.2f} pp[/]"


if __name__ == "__main__":
    app()
