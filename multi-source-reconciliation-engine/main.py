"""CLI entry point for the reconciliation engine."""
import sys
from pathlib import Path

import click

from src.pipeline import run_pipeline


@click.command()
@click.option("--core-banking",    required=True, type=click.Path(exists=True), help="Core banking CSV/XLSX/JSON")
@click.option("--reporting",       required=True, type=click.Path(exists=True), help="Reporting system file")
@click.option("--aggregator",      required=True, type=click.Path(exists=True), help="Third-party aggregator file")
@click.option("--manual",          required=True, type=click.Path(exists=True), help="Manual entries file")
@click.option("--config",          default="config/settings.yaml", show_default=True, help="Config file path")
@click.option("--run-id",          default=None, help="Optional run identifier")
def cli(core_banking, reporting, aggregator, manual, config, run_id):
    """Multi-Source Financial Reconciliation Engine.

    Ingests data from four sources, runs fuzzy multi-key matching,
    classifies discrepancies, and produces a reconciliation report
    — all within a configurable SLA.
    """
    result = run_pipeline(
        source_paths={
            "core_banking": core_banking,
            "reporting_system": reporting,
            "third_party_aggregator": aggregator,
            "manual_entries": manual,
        },
        config_path=Path(config),
        run_id=run_id,
    )
    sys.exit(0 if result["sla_met"] else 1)


if __name__ == "__main__":
    cli()
