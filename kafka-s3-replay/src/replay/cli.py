"""CLI entry-point — `replay` command."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import click
import structlog
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from replay.engine.engine import ReplayEngine
from replay.engine.window import parse_window, window_from_days_ago
from replay.models import (
    ArchiveFormat,
    FileTargetConfig,
    HttpTargetConfig,
    KafkaTargetConfig,
    ReplayConfig,
    ReplayStatus,
    S3ArchiveConfig,
    TargetType,
    TimeWindow,
)
from replay.targets.factory import make_target

console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(level),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer() if verbose else structlog.processors.JSONRenderer(),
        ],
    )


# ─────────────────────────────────────────────
# Main group
# ─────────────────────────────────────────────

@click.group()
@click.version_option()
def main() -> None:
    """kafka-s3-replay — Disaster Recovery & Event Replay System."""


# ─────────────────────────────────────────────
# `replay run` — execute a replay job
# ─────────────────────────────────────────────

@main.command("run")
@click.option("--config", "-c", "config_file", type=click.Path(exists=True),
              help="Path to YAML/JSON replay config file.")
@click.option("--topics", "-t", multiple=True, required=False,
              help="Topic(s) to replay. Repeatable: -t orders -t payments")
@click.option("--bucket", "-b", envvar="REPLAY_S3_BUCKET",
              help="S3 bucket containing the archive.")
@click.option("--prefix", default="", show_default=True,
              help="S3 key prefix (folder) within the bucket.")
@click.option("--start", "start_ts",
              help="Window start (ISO-8601). E.g. 2024-03-01T00:00:00Z")
@click.option("--end", "end_ts",
              help="Window end (ISO-8601). Defaults to now.")
@click.option("--days", type=int,
              help="Shorthand: replay the last N days (max 30). Overrides --start/--end.")
@click.option("--target", "target_type",
              type=click.Choice(["kafka", "http", "file", "stdout"], case_sensitive=False),
              default="stdout", show_default=True,
              help="Downstream target.")
@click.option("--kafka-brokers", envvar="REPLAY_KAFKA_BROKERS",
              help="Kafka bootstrap servers (for --target kafka).")
@click.option("--http-url", envvar="REPLAY_HTTP_URL",
              help="Webhook URL (for --target http).")
@click.option("--output-file", type=click.Path(),
              help="Output file path (for --target file).")
@click.option("--rate-limit", type=float, default=None,
              help="Max events per second (default: unlimited).")
@click.option("--dry-run", is_flag=True,
              help="Parse and count events but do NOT send them.")
@click.option("--resume/--no-resume", default=True, show_default=True,
              help="Resume from checkpoint if one exists.")
@click.option("--job-id", default=None,
              help="Explicit job ID (auto-generated if omitted).")
@click.option("--format", "archive_fmt",
              type=click.Choice(["jsonl", "avro", "parquet"], case_sensitive=False),
              default="jsonl", show_default=True,
              help="Archive file format.")
@click.option("--region", default="us-east-1", show_default=True,
              help="AWS region.")
@click.option("--endpoint-url", default=None, envvar="AWS_ENDPOINT_URL",
              help="Custom S3 endpoint (MinIO / LocalStack).")
@click.option("--verbose", "-v", is_flag=True)
def run_cmd(
    config_file, topics, bucket, prefix, start_ts, end_ts, days,
    target_type, kafka_brokers, http_url, output_file,
    rate_limit, dry_run, resume, job_id, archive_fmt,
    region, endpoint_url, verbose,
) -> None:
    """Execute a replay job."""
    _setup_logging(verbose)

    # ── Load config from file or CLI flags ──────────────────────────────
    if config_file:
        cfg = _load_config_file(config_file)
    else:
        if not bucket:
            raise click.UsageError("--bucket is required when not using --config")
        if not topics:
            raise click.UsageError("--topics is required when not using --config")

        window = _build_window(days, start_ts, end_ts)
        cfg = _build_config_from_flags(
            topics=list(topics),
            bucket=bucket,
            prefix=prefix,
            window=window,
            target_type=target_type,
            kafka_brokers=kafka_brokers,
            http_url=http_url,
            output_file=output_file,
            rate_limit=rate_limit,
            dry_run=dry_run,
            job_id=job_id or _make_job_id(),
            archive_fmt=archive_fmt,
            region=region,
            endpoint_url=endpoint_url,
        )

    if not resume:
        from replay.engine.checkpoint import CheckpointStore
        CheckpointStore(cfg.checkpoint_dir, cfg.job_id).reset()

    _print_job_banner(cfg)

    asyncio.run(_run_with_progress(cfg))


# ─────────────────────────────────────────────
# `replay manifest` — list files without running
# ─────────────────────────────────────────────

@main.command("manifest")
@click.option("--topics", "-t", multiple=True, required=True)
@click.option("--bucket", "-b", required=True, envvar="REPLAY_S3_BUCKET")
@click.option("--prefix", default="")
@click.option("--days", type=int)
@click.option("--start", "start_ts")
@click.option("--end", "end_ts")
@click.option("--region", default="us-east-1")
@click.option("--endpoint-url", default=None, envvar="AWS_ENDPOINT_URL")
@click.option("--output", "-o", type=click.Path(),
              help="Write manifest JSON to this file.")
def manifest_cmd(topics, bucket, prefix, days, start_ts, end_ts, region, endpoint_url, output):
    """List all S3 archive files matching the window and write a manifest."""
    from replay.archive.manifest import build_manifest

    window = _build_window(days, start_ts, end_ts)
    archive_cfg = S3ArchiveConfig(
        bucket=bucket,
        prefix=prefix,
        region=region,
        endpoint_url=endpoint_url,
    )

    async def _run():
        entries = await build_manifest(list(topics), window, archive_cfg, output_path=output)
        table = Table(title="Replay Manifest", show_lines=True)
        table.add_column("Topic", style="cyan")
        table.add_column("S3 Key", style="white")
        for e in entries:
            table.add_row(e.topic, e.key)
        console.print(table)
        console.print(f"\n[bold green]{len(entries)} files found[/bold green]")

    asyncio.run(_run())


# ─────────────────────────────────────────────
# `replay status` — inspect a checkpoint
# ─────────────────────────────────────────────

@main.command("status")
@click.argument("job_id")
@click.option("--checkpoint-dir", default="/tmp/replay-checkpoints", show_default=True)
def status_cmd(job_id, checkpoint_dir):
    """Show checkpoint status for a job."""
    from pathlib import Path
    path = Path(checkpoint_dir) / f"{job_id}.json"
    if not path.exists():
        console.print(f"[red]No checkpoint found for job '{job_id}'[/red]")
        raise SystemExit(1)
    data = json.loads(path.read_text())
    console.print_json(json.dumps(data, indent=2))


# ─────────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────────

def _build_window(days, start_ts, end_ts) -> TimeWindow:
    if days:
        return window_from_days_ago(days)
    if start_ts:
        end = end_ts or datetime.now(tz=timezone.utc).isoformat()
        return parse_window(start_ts, end)
    raise click.UsageError("Provide either --days or --start (and optionally --end).")


def _build_config_from_flags(**kw) -> ReplayConfig:
    archive = S3ArchiveConfig(
        bucket=kw["bucket"],
        prefix=kw["prefix"],
        region=kw["region"],
        format=ArchiveFormat(kw["archive_fmt"]),
        endpoint_url=kw["endpoint_url"],
    )

    tt = TargetType(kw["target_type"].lower())
    kafka_cfg = http_cfg = file_cfg = None

    if tt == TargetType.KAFKA:
        if not kw["kafka_brokers"]:
            raise click.UsageError("--kafka-brokers required for kafka target")
        kafka_cfg = KafkaTargetConfig(bootstrap_servers=kw["kafka_brokers"])

    elif tt == TargetType.HTTP:
        if not kw["http_url"]:
            raise click.UsageError("--http-url required for http target")
        http_cfg = HttpTargetConfig(url=kw["http_url"])

    elif tt == TargetType.FILE:
        if not kw["output_file"]:
            raise click.UsageError("--output-file required for file target")
        file_cfg = FileTargetConfig(path=kw["output_file"])

    return ReplayConfig(
        job_id=kw["job_id"],
        topics=kw["topics"],
        window=kw["window"],
        archive=archive,
        target_type=tt,
        kafka_target=kafka_cfg,
        http_target=http_cfg,
        file_target=file_cfg,
        rate_limit_per_second=kw["rate_limit"],
        dry_run=kw["dry_run"],
    )


def _load_config_file(path: str) -> ReplayConfig:
    import yaml  # optional dep; only needed for YAML config files
    raw = Path(path).read_text()
    if path.endswith((".yaml", ".yml")):
        data = yaml.safe_load(raw)
    else:
        data = json.loads(raw)

    # Coerce window dict to TimeWindow
    if "window" in data and isinstance(data["window"], dict):
        data["window"] = TimeWindow(**data["window"])

    return ReplayConfig.model_validate(data)


def _make_job_id() -> str:
    return f"replay-{datetime.now(tz=timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"


def _print_job_banner(cfg: ReplayConfig) -> None:
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(style="bold cyan", no_wrap=True)
    table.add_column(style="white")
    table.add_row("Job ID", cfg.job_id)
    table.add_row("Topics", ", ".join(cfg.topics))
    table.add_row("Window start", cfg.window.start.strftime("%Y-%m-%d %H:%M UTC"))
    table.add_row("Window end", cfg.window.end.strftime("%Y-%m-%d %H:%M UTC"))
    table.add_row("Duration", f"{cfg.window.duration_hours:.1f} h")
    table.add_row("Target", cfg.target_type.value.upper())
    table.add_row("Archive", f"s3://{cfg.archive.bucket}/{cfg.archive.prefix or ''}*")
    table.add_row("Rate limit", f"{cfg.rate_limit_per_second} evt/s" if cfg.rate_limit_per_second else "unlimited")
    table.add_row("Dry run", "YES ⚠️" if cfg.dry_run else "no")
    console.print(Panel(table, title="[bold]Replay Job[/bold]", border_style="blue"))


async def _run_with_progress(cfg: ReplayConfig) -> None:
    target = make_target(cfg)
    engine = ReplayEngine(cfg, target)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    )

    task_id = progress.add_task("Replaying events…", total=None)

    async def consume_progress() -> None:
        while True:
            try:
                p = engine.progress_queue.get_nowait()
                progress.update(
                    task_id,
                    total=p.total_events or None,
                    completed=p.replayed_events,
                    description=(
                        f"[{'green' if p.status == ReplayStatus.RUNNING else 'yellow'}]"
                        f"{p.status.value.upper()}[/] "
                        f"✓ {p.replayed_events:,}  ✗ {p.failed_events:,}  ⤼ {p.skipped_events:,}"
                    ),
                )
                if p.status in (ReplayStatus.COMPLETED, ReplayStatus.FAILED, ReplayStatus.PAUSED):
                    return
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.2)

    with Live(progress, console=console, refresh_per_second=4):
        replay_task = asyncio.create_task(engine.run())
        progress_task = asyncio.create_task(consume_progress())

        try:
            result = await replay_task
        except Exception as exc:
            console.print(f"\n[bold red]Replay failed:[/bold red] {exc}")
            progress_task.cancel()
            raise SystemExit(1)

        await progress_task

    _print_summary(result)


def _print_summary(p) -> None:
    colour = "green" if p.status == ReplayStatus.COMPLETED else "red"
    elapsed = ""
    if p.started_at and p.completed_at:
        secs = (p.completed_at - p.started_at).total_seconds()
        elapsed = f"  •  {secs:.1f}s"

    console.print(
        f"\n[bold {colour}]{p.status.value.upper()}[/bold {colour}]"
        f"  •  {p.replayed_events:,} replayed"
        f"  •  {p.failed_events:,} failed"
        f"  •  {p.skipped_events:,} skipped"
        f"{elapsed}"
    )
