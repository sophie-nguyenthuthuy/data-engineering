#!/usr/bin/env python3
"""
Tiered Storage Orchestrator — CLI

Usage examples:
  python cli.py put user:1 '{"name":"Alice"}'
  python cli.py get user:1
  python cli.py locate user:1
  python cli.py delete user:1
  python cli.py metrics
  python cli.py cost
  python cli.py savings
  python cli.py lifecycle
  python cli.py rehydrate user:1 --priority expedited
  python cli.py sla-report
"""
from __future__ import annotations

import asyncio
import json
import sys

import click

from tiered_storage.config import StorageConfig
from tiered_storage.orchestrator import TieredStorageOrchestrator
from tiered_storage.schemas import RehydrationPriority
from tiered_storage.tiers.cold import ColdTier
from tiered_storage.tiers.hot import HotTier
from tiered_storage.tiers.warm import WarmTier


def _build_orch(cold_local: str | None) -> TieredStorageOrchestrator:
    """Build orchestrator; override cold path for local dev if set."""
    cfg = StorageConfig()
    if cold_local:
        cfg.cold_local_path = cold_local

    # Use lightweight in-process stubs when real services aren't available
    hot = HotTier(redis_url=cfg.redis_url, postgres_dsn=cfg.postgres_dsn)
    warm = WarmTier(
        bucket=cfg.s3_bucket,
        prefix=cfg.s3_warm_prefix,
        region=cfg.s3_region,
        endpoint_url=cfg.s3_endpoint_url,
    )
    cold = ColdTier(
        bucket=cfg.s3_bucket if not cfg.cold_local_path else None,
        local_path=cfg.cold_local_path,
        prefix=cfg.s3_cold_prefix,
        region=cfg.s3_region,
        endpoint_url=cfg.s3_endpoint_url,
    )
    return TieredStorageOrchestrator(
        config=cfg,
        hot_tier=hot,
        warm_tier=warm,
        cold_tier=cold,
    )


@click.group()
@click.option("--cold-local", default=None, envvar="COLD_LOCAL_PATH",
              help="Local directory path for cold tier (dev mode).")
@click.pass_context
def cli(ctx: click.Context, cold_local: str | None) -> None:
    """Tiered Storage Orchestrator CLI."""
    ctx.ensure_object(dict)
    ctx.obj["cold_local"] = cold_local


# -----------------------------------------------------------------------
# put
# -----------------------------------------------------------------------
@cli.command()
@click.argument("key")
@click.argument("value_json")
@click.option("--meta", default=None, help="JSON metadata dict.")
@click.pass_context
def put(ctx: click.Context, key: str, value_json: str, meta: str | None) -> None:
    """Write KEY → VALUE_JSON to hot tier."""
    orch = _build_orch(ctx.obj["cold_local"])

    async def _run() -> None:
        await orch.start(run_lifecycle=False)
        try:
            value = json.loads(value_json)
            metadata = json.loads(meta) if meta else {}
            record = await orch.put(key, value, metadata=metadata)
            click.echo(f"✓ Stored  key={record.key}  tier={record.tier.value}  size={record.size_bytes}B")
        finally:
            await orch.stop()

    asyncio.run(_run())


# -----------------------------------------------------------------------
# get
# -----------------------------------------------------------------------
@cli.command()
@click.argument("key")
@click.option("--priority", default="standard",
              type=click.Choice(["expedited", "standard", "bulk"]))
@click.option("--block", is_flag=True, default=False,
              help="Block until cold data is rehydrated.")
@click.pass_context
def get(ctx: click.Context, key: str, priority: str, block: bool) -> None:
    """Read KEY from the appropriate tier."""
    orch = _build_orch(ctx.obj["cold_local"])

    async def _run() -> None:
        await orch.start(run_lifecycle=False)
        try:
            result = await orch.get(
                key,
                cold_priority=RehydrationPriority(priority),
                block_on_cold=block,
            )
            if result.record:
                click.echo(f"tier={result.tier_hit.value}  latency={result.latency_ms:.2f}ms")
                click.echo(json.dumps(result.record.value, indent=2))
            elif result.rehydration_job:
                job = result.rehydration_job
                click.echo(
                    f"⏳  Cold restore queued — job_id={job.job_id}\n"
                    f"    priority={job.priority.value}  "
                    f"SLA in {job.eta_seconds:.0f}s"
                )
            else:
                click.echo(f"✗ Key '{key}' not found in any tier.", err=True)
                sys.exit(1)
        finally:
            await orch.stop()

    asyncio.run(_run())


# -----------------------------------------------------------------------
# locate
# -----------------------------------------------------------------------
@cli.command()
@click.argument("key")
@click.pass_context
def locate(ctx: click.Context, key: str) -> None:
    """Show which tier holds KEY."""
    orch = _build_orch(ctx.obj["cold_local"])

    async def _run() -> None:
        await orch.start(run_lifecycle=False)
        try:
            tier = await orch.locate(key)
            click.echo(f"key={key}  tier={tier.value}")
        finally:
            await orch.stop()

    asyncio.run(_run())


# -----------------------------------------------------------------------
# delete
# -----------------------------------------------------------------------
@cli.command()
@click.argument("key")
@click.pass_context
def delete(ctx: click.Context, key: str) -> None:
    """Remove KEY from all tiers."""
    orch = _build_orch(ctx.obj["cold_local"])

    async def _run() -> None:
        await orch.start(run_lifecycle=False)
        try:
            found = await orch.delete(key)
            if found:
                click.echo(f"✓ Deleted key={key}")
            else:
                click.echo(f"✗ Key '{key}' not found.", err=True)
        finally:
            await orch.stop()

    asyncio.run(_run())


# -----------------------------------------------------------------------
# metrics
# -----------------------------------------------------------------------
@cli.command()
@click.pass_context
def metrics(ctx: click.Context) -> None:
    """Print live metrics for all tiers."""
    orch = _build_orch(ctx.obj["cold_local"])

    async def _run() -> None:
        await orch.start(run_lifecycle=False)
        try:
            m = await orch.metrics()
            click.echo(json.dumps(m, indent=2))
        finally:
            await orch.stop()

    asyncio.run(_run())


# -----------------------------------------------------------------------
# cost
# -----------------------------------------------------------------------
@cli.command()
@click.option("--hot-reads", default=1000.0, show_default=True)
@click.option("--warm-reads", default=100.0, show_default=True)
@click.option("--cold-reads", default=10.0, show_default=True)
@click.option("--egress-gb", default=1.0, show_default=True)
@click.pass_context
def cost(
    ctx: click.Context,
    hot_reads: float,
    warm_reads: float,
    cold_reads: float,
    egress_gb: float,
) -> None:
    """Predict monthly storage cost across all tiers."""
    orch = _build_orch(ctx.obj["cold_local"])

    async def _run() -> None:
        await orch.start(run_lifecycle=False)
        try:
            breakdown = await orch.cost_report(
                hot_reads_per_day=hot_reads,
                warm_reads_per_day=warm_reads,
                cold_reads_per_day=cold_reads,
                egress_gb_per_day=egress_gb,
            )
            click.echo("\n── Monthly Cost Estimate ────────────────────")
            click.echo(breakdown.summary())
            click.echo()
        finally:
            await orch.stop()

    asyncio.run(_run())


# -----------------------------------------------------------------------
# savings
# -----------------------------------------------------------------------
@cli.command()
@click.pass_context
def savings(ctx: click.Context) -> None:
    """Show potential savings from current tier distribution."""
    orch = _build_orch(ctx.obj["cold_local"])

    async def _run() -> None:
        await orch.start(run_lifecycle=False)
        try:
            report = await orch.savings_report()
            click.echo(json.dumps(report, indent=2))
        finally:
            await orch.stop()

    asyncio.run(_run())


# -----------------------------------------------------------------------
# lifecycle
# -----------------------------------------------------------------------
@cli.command()
@click.pass_context
def lifecycle(ctx: click.Context) -> None:
    """Manually trigger one lifecycle scan cycle."""
    orch = _build_orch(ctx.obj["cold_local"])

    async def _run() -> None:
        await orch.start(run_lifecycle=False)
        try:
            report = await orch.run_lifecycle_cycle()
            click.echo(report.summary())
            if report.hot_to_warm:
                click.echo(f"\nHot → Warm ({len(report.hot_to_warm)}):")
                for m in report.hot_to_warm:
                    click.echo(f"  {m.key}  reason={m.reason}  size={m.size_bytes}B")
            if report.warm_to_cold:
                click.echo(f"\nWarm → Cold ({len(report.warm_to_cold)}):")
                for m in report.warm_to_cold:
                    click.echo(f"  {m.key}  reason={m.reason}  size={m.size_bytes}B")
            if report.errors:
                click.echo(f"\nErrors ({len(report.errors)}):", err=True)
                for e in report.errors:
                    click.echo(f"  {e}", err=True)
        finally:
            await orch.stop()

    asyncio.run(_run())


# -----------------------------------------------------------------------
# rehydrate
# -----------------------------------------------------------------------
@cli.command()
@click.argument("key")
@click.option("--priority", default="standard",
              type=click.Choice(["expedited", "standard", "bulk"]))
@click.option("--block", is_flag=True, default=False)
@click.pass_context
def rehydrate(ctx: click.Context, key: str, priority: str, block: bool) -> None:
    """Trigger rehydration of a cold KEY."""
    orch = _build_orch(ctx.obj["cold_local"])

    async def _run() -> None:
        await orch.start(run_lifecycle=False)
        try:
            job = await orch.rehydrate(
                key,
                priority=RehydrationPriority(priority),
                block=block,
            )
            status = "complete" if job.completed_at else "in-flight"
            sla = "MET" if job.sla_met else ("PENDING" if not job.completed_at else "VIOLATED")
            click.echo(
                f"job_id={job.job_id}  status={status}  "
                f"priority={job.priority.value}  SLA={sla}"
            )
            if block and job.completed_at:
                latency = job.completed_at - job.requested_at
                click.echo(f"Restore latency: {latency:.2f}s")
        finally:
            await orch.stop()

    asyncio.run(_run())


# -----------------------------------------------------------------------
# sla-report
# -----------------------------------------------------------------------
@cli.command("sla-report")
@click.pass_context
def sla_report(ctx: click.Context) -> None:
    """Show rehydration SLA compliance statistics."""
    orch = _build_orch(ctx.obj["cold_local"])

    async def _run() -> None:
        await orch.start(run_lifecycle=False)
        try:
            report = orch.rehydration.sla_report()
            click.echo(json.dumps(report, indent=2))
        finally:
            await orch.stop()

    asyncio.run(_run())


if __name__ == "__main__":
    cli()
