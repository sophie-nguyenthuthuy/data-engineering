"""streamdigest CLI.

Commands:
  streamdigest init          create DuckDB schema
  streamdigest run           ingest + enrich (runs the Prefect flow)
  streamdigest digest        print the top-priority enriched rows
  streamdigest evals         run the eval harness against Ollama
  streamdigest doctor        check env + Ollama + DuckDB reachability
"""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from streamdigest.ai import OllamaClient
from streamdigest.config import settings
from streamdigest.storage import get_store

app = typer.Typer(add_completion=False, help="streamdigest — local-first event digest")
console = Console()


@app.command()
def init() -> None:
    """Create the DuckDB enriched schema (idempotent)."""
    store = get_store()
    store.init_schema()
    store.ensure_digest_view()
    console.print(f"[green]Initialized[/green] {settings.duckdb_path}")


@app.command()
def run(batch_size: int = typer.Option(50, help="Max notifications to enrich per run")) -> None:
    """Ingest from GitHub and enrich with Ollama."""
    from streamdigest.flows.ingest_and_enrich import ingest_and_enrich

    result = ingest_and_enrich(batch_size=batch_size)
    console.print(result)


@app.command()
def digest(limit: int = 20) -> None:
    """Show top-priority enriched notifications."""
    store = get_store()
    store.ensure_digest_view()
    rows = store.top_digest(limit=limit)
    if not rows:
        console.print("[yellow]No enriched rows yet. Run `streamdigest run` first.[/yellow]")
        return

    table = Table(show_lines=True)
    for col in ["P", "repo", "type", "title", "summary", "actions"]:
        table.add_column(col, overflow="fold")
    for r in rows:
        actions = r["actions"] or "[]"
        table.add_row(
            str(r["priority"]),
            r["repo"] or "",
            r["type"] or "",
            r["title"] or "",
            r["summary"] or "",
            str(actions),
        )
    console.print(table)


@app.command()
def evals() -> None:
    """Run the eval harness."""
    from streamdigest.evals.run_evals import run as run_evals

    raise typer.Exit(code=run_evals())


@app.command()
def doctor() -> None:
    """Check that everything the pipeline needs is reachable."""
    ok = True

    console.print("[bold]Config[/bold]")
    console.print(f"  DuckDB path: {settings.duckdb_path}")
    console.print(f"  Ollama:      {settings.ollama_host}  (model={settings.ollama_model})")
    console.print(f"  GITHUB_TOKEN set: {'yes' if settings.github_token else 'no'}")
    if not settings.github_token:
        console.print("  [red]GITHUB_TOKEN missing — ingestion will fail.[/red]")
        ok = False

    console.print("\n[bold]Ollama[/bold]")
    with OllamaClient() as c:
        if c.ping():
            console.print("  [green]reachable[/green]")
        else:
            console.print(f"  [red]unreachable at {c.host}[/red]")
            ok = False

    console.print("\n[bold]DuckDB[/bold]")
    try:
        store = get_store()
        store.init_schema()
        console.print(f"  [green]writable[/green] at {settings.duckdb_path}")
    except Exception as e:
        console.print(f"  [red]{e}[/red]")
        ok = False

    raise typer.Exit(code=0 if ok else 1)


if __name__ == "__main__":
    app()
