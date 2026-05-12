import signal
import sys
import time
import click
import uvicorn
from rich.console import Console
from rich.table import Table
from .pipeline import load_all_pipelines, run_pipeline
from .scheduler.scheduler import PipelineScheduler
from .triggers.webhook import create_webhook_app
from .settings import settings
from .logger import get_logger

logger = get_logger(__name__)
console = Console()


@click.group()
def cli() -> None:
    """Reverse ETL — sync analytics results into operational tools."""


@cli.command("run")
@click.argument("pipeline_name")
@click.option("--config-dir", default=settings.pipeline_config_dir, show_default=True)
def run_cmd(pipeline_name: str, config_dir: str) -> None:
    """Run a single pipeline by name immediately."""
    pipelines = {p.name: p for p in load_all_pipelines(config_dir)}
    config = pipelines.get(pipeline_name)
    if not config:
        console.print(f"[red]Pipeline '{pipeline_name}' not found in {config_dir}[/red]")
        sys.exit(1)

    result = run_pipeline(config)
    if result.success:
        console.print(f"[green]✓ {result.pipeline}: {result.rows_extracted} extracted, {result.rows_synced} synced[/green]")
    else:
        console.print(f"[red]✗ {result.pipeline}: {result.error}[/red]")
        sys.exit(1)


@cli.command("run-all")
@click.option("--config-dir", default=settings.pipeline_config_dir, show_default=True)
def run_all_cmd(config_dir: str) -> None:
    """Run all enabled pipelines immediately."""
    pipelines = load_all_pipelines(config_dir)
    enabled = [p for p in pipelines if p.enabled]
    console.print(f"Running {len(enabled)} pipeline(s)...")

    for config in enabled:
        result = run_pipeline(config)
        status = "[green]✓[/green]" if result.success else "[red]✗[/red]"
        console.print(f"  {status} {result.pipeline}: extracted={result.rows_extracted}, synced={result.rows_synced}")


@cli.command("list")
@click.option("--config-dir", default=settings.pipeline_config_dir, show_default=True)
def list_cmd(config_dir: str) -> None:
    """List all configured pipelines."""
    pipelines = load_all_pipelines(config_dir)
    table = Table(title="Pipelines")
    table.add_column("Name", style="bold")
    table.add_column("Enabled")
    table.add_column("Source")
    table.add_column("Destination")
    table.add_column("Schedule")
    table.add_column("Description")

    for p in pipelines:
        sched = ""
        if p.schedule:
            sched = p.schedule.cron or f"every {p.schedule.seconds}s"
        table.add_row(
            p.name,
            "[green]yes[/green]" if p.enabled else "[dim]no[/dim]",
            p.source.type,
            p.destination.type,
            sched,
            p.description,
        )
    console.print(table)


@cli.command("schedule")
@click.option("--config-dir", default=settings.pipeline_config_dir, show_default=True)
def schedule_cmd(config_dir: str) -> None:
    """Start the scheduler (blocking). Runs pipelines on their configured schedules."""
    pipelines = load_all_pipelines(config_dir)
    scheduler = PipelineScheduler()
    scheduler.register_all(pipelines)
    scheduler.start()

    def _shutdown(sig, frame):
        console.print("\n[yellow]Shutting down scheduler...[/yellow]")
        scheduler.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    jobs = scheduler.list_jobs()
    console.print(f"[bold green]Scheduler running — {len(jobs)} job(s)[/bold green]")
    for job in jobs:
        console.print(f"  • {job['name']} → next run: {job['next_run']}")

    while True:
        time.sleep(10)


@cli.command("serve")
@click.option("--config-dir", default=settings.pipeline_config_dir, show_default=True)
@click.option("--host", default=settings.api_host, show_default=True)
@click.option("--port", default=settings.api_port, show_default=True)
@click.option("--with-scheduler", is_flag=True, default=False, help="Also run the scheduler")
def serve_cmd(config_dir: str, host: str, port: int, with_scheduler: bool) -> None:
    """Start the webhook API server (optionally with the scheduler)."""
    pipelines = load_all_pipelines(config_dir)
    app = create_webhook_app(pipelines)

    scheduler: PipelineScheduler | None = None
    if with_scheduler:
        scheduler = PipelineScheduler()
        scheduler.register_all(pipelines)
        scheduler.start()

    try:
        uvicorn.run(app, host=host, port=port, log_level=settings.log_level.lower())
    finally:
        if scheduler:
            scheduler.stop()


if __name__ == "__main__":
    cli()
