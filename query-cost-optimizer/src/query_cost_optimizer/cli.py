"""CLI entry point — `qco` command."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import click
from dotenv import load_dotenv
from rich.console import Console

load_dotenv()
console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(format="%(levelname)s  %(name)s  %(message)s", level=level)


# ─── Root group ──────────────────────────────────────────────────────────────

@click.group()
@click.version_option(package_name="query-cost-optimizer")
def main() -> None:
    """⚡ Query Cost & Performance Optimization Engine.

    Analyses BigQuery / Snowflake query history and recommends:
    \b
      • Clustering keys
      • Partitioning strategies
      • Expensive SQL pattern fixes

    Run `qco bigquery --help` or `qco snowflake --help` to get started.
    """


# ─── Shared output options ────────────────────────────────────────────────────

_output_options = [
    click.option("--days", default=30, show_default=True, help="Days of query history to analyse."),
    click.option("--min-savings", default=10.0, show_default=True, help="Min monthly USD savings to surface a recommendation."),
    click.option("--min-queries", default=5, show_default=True, help="Min query count to flag a table or pattern."),
    click.option("--output", "-o", type=click.Choice(["console", "json", "html", "all"]), default="console", show_default=True, help="Output format."),
    click.option("--out-dir", default="./reports", show_default=True, help="Directory for json/html output files."),
    click.option("--verbose", "-v", is_flag=True, help="Enable debug logging."),
]


def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func
    return _add_options


# ─── BigQuery command ─────────────────────────────────────────────────────────

@main.command()
@click.option("--project", envvar="BQ_PROJECT_ID", required=True, help="GCP project ID (or set BQ_PROJECT_ID).")
@add_options(_output_options)
def bigquery(project, days, min_savings, min_queries, output, out_dir, verbose):
    """Analyse BigQuery query history and surface optimisation recommendations."""
    _setup_logging(verbose)
    console.print(f"[cyan]Connecting to BigQuery project:[/cyan] [bold]{project}[/bold]")
    try:
        from .engine import run_bigquery
        report = run_bigquery(
            project_id=project,
            history_days=days,
            min_savings_usd=min_savings,
            min_query_count=min_queries,
        )
    except ImportError as e:
        console.print(f"[red]Missing dependency:[/red] {e}")
        console.print("Install with: [bold]pip install 'query-cost-optimizer[bigquery]'[/bold]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        if verbose:
            raise
        sys.exit(1)

    _emit(report, output, out_dir, "bq")


# ─── Snowflake command ────────────────────────────────────────────────────────

@main.command()
@click.option("--account", envvar="SNOWFLAKE_ACCOUNT", required=True, help="Snowflake account identifier (or set SNOWFLAKE_ACCOUNT).")
@click.option("--user", envvar="SNOWFLAKE_USER", required=True, help="Snowflake username (or set SNOWFLAKE_USER).")
@click.option("--password", envvar="SNOWFLAKE_PASSWORD", default=None, help="Snowflake password (or set SNOWFLAKE_PASSWORD).")
@click.option("--warehouse", envvar="SNOWFLAKE_WAREHOUSE", default=None, help="Snowflake warehouse to use.")
@add_options(_output_options)
def snowflake(account, user, password, warehouse, days, min_savings, min_queries, output, out_dir, verbose):
    """Analyse Snowflake query history and surface optimisation recommendations."""
    _setup_logging(verbose)
    console.print(f"[cyan]Connecting to Snowflake account:[/cyan] [bold]{account}[/bold]")
    try:
        from .engine import run_snowflake
        report = run_snowflake(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            history_days=days,
            min_savings_usd=min_savings,
            min_query_count=min_queries,
        )
    except ImportError as e:
        console.print(f"[red]Missing dependency:[/red] {e}")
        console.print("Install with: [bold]pip install 'query-cost-optimizer[snowflake]'[/bold]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        if verbose:
            raise
        sys.exit(1)

    _emit(report, output, out_dir, "sf")


# ─── Demo command (no real warehouse needed) ──────────────────────────────────

@main.command()
@click.option("--platform", type=click.Choice(["bigquery", "snowflake"]), default="bigquery", show_default=True)
@click.option("--output", "-o", type=click.Choice(["console", "json", "html", "all"]), default="console", show_default=True)
@click.option("--out-dir", default="./reports", show_default=True)
def demo(platform, output, out_dir):
    """Run the optimizer against synthetic demo data (no credentials needed)."""
    from .demo import build_demo_report
    report = build_demo_report(platform)
    _emit(report, output, out_dir, "demo")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _emit(report, output: str, out_dir: str, prefix: str) -> None:
    from .reporters.report import ConsoleReporter, JsonReporter, HtmlReporter

    ts = report.generated_at.strftime("%Y%m%d_%H%M%S")
    base = Path(out_dir) / f"{prefix}_{ts}"

    if output in ("console", "all"):
        ConsoleReporter().render(report)
    if output in ("json", "all"):
        JsonReporter().render(report, base.with_suffix(".json"))
    if output in ("html", "all"):
        HtmlReporter().render(report, base.with_suffix(".html"))
