#!/usr/bin/env python3
"""
mv-selector CLI

Usage examples:

  # Analyse a local worklog file and print recommendations (dry-run)
  mv-selector analyse --worklog queries.jsonl --budget-gb 200

  # Run a full optimisation cycle against BigQuery
  mv-selector run --warehouse bigquery --project my-gcp-project --dataset analytics.mv_auto

  # Show status of live views and calibration
  mv-selector status

  # Import a JSONL query log into the local worklog store
  mv-selector import --file queries.jsonl --warehouse bigquery
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    level=logging.INFO,
    stream=sys.stderr,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_jsonl(path: Path) -> list[dict]:
    records = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _print_result(result) -> None:
    click.echo(
        f"\n{'─'*60}\n"
        f"  Algorithm      : {result.algorithm}\n"
        f"  Views selected : {len(result.selected)}\n"
        f"  Storage used   : {result.total_storage_bytes / 1024**3:.1f} GB\n"
        f"  Est. benefit   : ${result.total_estimated_benefit_usd:.2f}/mo\n"
        f"  Maintenance    : ${result.total_maintenance_cost_usd:.2f}/mo\n"
        f"  Net benefit    : ${result.net_benefit_usd:.2f}/mo\n"
        f"  SA iterations  : {result.iterations:,}\n"
        f"  Elapsed        : {result.elapsed_seconds:.2f}s\n"
        f"{'─'*60}"
    )
    for v in result.selected:
        click.echo(
            f"  + {v.name:<30}  benefit=${v.estimated_benefit_usd:7.2f}  "
            f"storage={v.estimated_storage_bytes / 1024**2:.0f} MB  "
            f"tables={','.join(v.referenced_tables[:3])}"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.group()
@click.option("--debug", is_flag=True, default=False)
def cli(debug: bool) -> None:
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)


# ── analyse ─────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--worklog", type=click.Path(exists=True), required=True,
              help="Path to JSONL worklog (one QueryRecord per line)")
@click.option("--warehouse", type=click.Choice(["bigquery", "snowflake"]),
              default="bigquery", show_default=True)
@click.option("--budget-gb", type=float, default=500.0, show_default=True)
@click.option("--min-freq", type=int, default=3, show_default=True,
              help="Minimum query frequency for a pattern to become a candidate")
@click.option("--sa-iters", type=int, default=50_000, show_default=True)
@click.option("--seed", type=int, default=None)
def analyse(
    worklog: str,
    warehouse: str,
    budget_gb: float,
    min_freq: int,
    sa_iters: int,
    seed: Optional[int],
) -> None:
    """Analyse a JSONL worklog and print view recommendations (no warehouse connection)."""
    from mv_selector.models import QueryRecord, Warehouse
    from mv_selector.query_analyzer import QueryAnalyzer
    from mv_selector.cost_model import CostModel
    from mv_selector.optimizer import AnnealingSelector, GreedySelector

    wh = Warehouse(warehouse)
    raw = _load_jsonl(Path(worklog))
    records: list[QueryRecord] = []
    for d in raw:
        d["warehouse"] = wh
        d.setdefault("executed_at", datetime.now(timezone.utc).isoformat())
        d["executed_at"] = datetime.fromisoformat(d["executed_at"])
        records.append(QueryRecord(**d))

    click.echo(f"Loaded {len(records)} query records")

    analyzer = QueryAnalyzer(min_query_frequency=min_freq)
    candidates = analyzer.analyse(records)
    click.echo(f"Found {len(candidates)} candidate views")

    model = CostModel()
    candidates = model.refresh_estimates(candidates, wh)

    budget_bytes = int(budget_gb * 1024**3)

    greedy = GreedySelector().select(candidates, budget_bytes)
    click.echo(f"\n[Greedy]  net benefit = ${greedy.net_benefit_usd:.2f}/mo")

    sa = AnnealingSelector(max_iterations=sa_iters, seed=seed)
    result = sa.select(candidates, budget_bytes, greedy_seed=greedy.selected)

    click.echo(f"[SA]      net benefit = ${result.net_benefit_usd:.2f}/mo  "
               f"(+{result.net_benefit_usd - greedy.net_benefit_usd:.2f} vs greedy)")
    _print_result(result)


# ── run ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--warehouse", type=click.Choice(["bigquery", "snowflake"]),
              required=True)
@click.option("--project", default=None, help="GCP project (BigQuery)")
@click.option("--account", default=None, help="Snowflake account")
@click.option("--user", default=None, help="Snowflake user")
@click.option("--password", default=None, envvar="SNOWFLAKE_PASSWORD")
@click.option("--dataset", default="analytics.mv_auto",
              help="BQ dataset or SF schema for views")
@click.option("--budget-gb", type=float, default=500.0)
@click.option("--dry-run", is_flag=True, default=False)
def run(
    warehouse: str,
    project: Optional[str],
    account: Optional[str],
    user: Optional[str],
    password: Optional[str],
    dataset: str,
    budget_gb: float,
    dry_run: bool,
) -> None:
    """Run a full optimisation cycle against a live warehouse."""
    from mv_selector.scheduler import ViewScheduler, SchedulerConfig

    if warehouse == "bigquery":
        from mv_selector.adapters.bigquery import BigQueryAdapter
        if not project:
            raise click.UsageError("--project is required for BigQuery")
        adapter = BigQueryAdapter(project=project)
    else:
        from mv_selector.adapters.snowflake import SnowflakeAdapter
        if not account or not user:
            raise click.UsageError("--account and --user are required for Snowflake")
        adapter = SnowflakeAdapter(account=account, user=user, password=password)

    cfg = SchedulerConfig(
        budget_bytes=int(budget_gb * 1024**3),
        target_dataset_or_schema=dataset,
    )

    if dry_run:
        click.echo("[dry-run] would run optimization cycle — skipping warehouse writes")
        return

    scheduler = ViewScheduler(adapter=adapter, config=cfg)
    result = scheduler.run_cycle()
    _print_result(result)


# ── status ───────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--warehouse", type=click.Choice(["bigquery", "snowflake"]),
              required=True)
@click.option("--project", default=None)
@click.option("--account", default=None)
@click.option("--user", default=None)
@click.option("--password", default=None, envvar="SNOWFLAKE_PASSWORD")
def status(
    warehouse: str,
    project: Optional[str],
    account: Optional[str],
    user: Optional[str],
    password: Optional[str],
) -> None:
    """Print status of live views and calibration."""
    from mv_selector.scheduler import ViewScheduler

    if warehouse == "bigquery":
        from mv_selector.adapters.bigquery import BigQueryAdapter
        if not project:
            raise click.UsageError("--project required")
        adapter = BigQueryAdapter(project=project)
    else:
        from mv_selector.adapters.snowflake import SnowflakeAdapter
        adapter = SnowflakeAdapter(account=account, user=user, password=password)

    s = ViewScheduler(adapter=adapter)
    info = s.status()
    click.echo(json.dumps(info, indent=2))


# ── import ───────────────────────────────────────────────────────────────────

@cli.command("import")
@click.option("--file", "filepath", type=click.Path(exists=True), required=True)
@click.option("--warehouse", type=click.Choice(["bigquery", "snowflake"]),
              default="bigquery")
def import_worklog(filepath: str, warehouse: str) -> None:
    """Import a JSONL query log into the local SQLite worklog store."""
    from mv_selector.models import QueryRecord, Warehouse
    from mv_selector.worklog import WorklogStore

    wh = Warehouse(warehouse)
    raw = _load_jsonl(Path(filepath))
    records = []
    for d in raw:
        d["warehouse"] = wh
        d["executed_at"] = datetime.fromisoformat(d.get("executed_at", datetime.now(timezone.utc).isoformat()))
        records.append(QueryRecord(**d))

    store = WorklogStore()
    inserted = store.upsert(records)
    click.echo(f"Imported {inserted} new records from {len(raw)} rows")
    click.echo(json.dumps(store.stats(), indent=2))


if __name__ == "__main__":
    cli()
