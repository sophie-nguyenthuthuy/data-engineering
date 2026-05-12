"""
Schema Registry CLI
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Optional

import httpx
import typer
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

app = typer.Typer(
    name="schema-registry",
    help="Schema Registry CLI — manage subjects, schemas, migrations, and replays.",
    no_args_is_help=True,
)
console = Console()

DEFAULT_HOST = "http://localhost:8000"


def get_client(host: str) -> httpx.Client:
    return httpx.Client(base_url=f"{host}/api/v1", timeout=30)


def fatal(msg: str) -> None:
    console.print(f"[bold red]Error:[/] {msg}")
    raise typer.Exit(1)


# ── Server ────────────────────────────────────────────────────────────────────

@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", help="Bind host"),
    port: int = typer.Option(8000, help="Bind port"),
    db: str = typer.Option("registry.db", help="SQLite DB path"),
    reload: bool = typer.Option(False, help="Enable hot-reload (dev)"),
):
    """Start the Schema Registry API server."""
    import uvicorn
    from src.api.app import create_app

    uvicorn.run(
        "src.api.app:create_app",
        factory=True,
        host=host,
        port=port,
        reload=reload,
    )


# ── Subjects ──────────────────────────────────────────────────────────────────

subjects_app = typer.Typer(help="Manage subjects")
app.add_typer(subjects_app, name="subjects")


@subjects_app.command("list")
def subjects_list(server: str = typer.Option(DEFAULT_HOST)):
    """List all subjects."""
    with get_client(server) as c:
        r = c.get("/subjects")
        r.raise_for_status()
    subjects = r.json()
    if not subjects:
        console.print("[dim]No subjects registered.[/]")
        return
    t = Table("Subject", title="Registered Subjects")
    for s in subjects:
        t.add_row(s)
    console.print(t)


@subjects_app.command("delete")
def subjects_delete(subject: str, server: str = typer.Option(DEFAULT_HOST)):
    """Delete a subject and all its versions."""
    with get_client(server) as c:
        r = c.delete(f"/subjects/{subject}")
        r.raise_for_status()
    console.print(f"[green]Deleted[/] subject '{subject}': {r.json()}")


# ── Schemas ───────────────────────────────────────────────────────────────────

schemas_app = typer.Typer(help="Manage schema versions")
app.add_typer(schemas_app, name="schemas")


@schemas_app.command("list")
def schemas_list(subject: str, server: str = typer.Option(DEFAULT_HOST)):
    """List versions for a subject."""
    with get_client(server) as c:
        r = c.get(f"/subjects/{subject}/versions")
        r.raise_for_status()
    console.print(f"Versions for [bold]{subject}[/]: {r.json()}")


@schemas_app.command("get")
def schemas_get(
    subject: str,
    version: str = typer.Argument("latest"),
    server: str = typer.Option(DEFAULT_HOST),
):
    """Get a schema version (default: latest)."""
    with get_client(server) as c:
        r = c.get(f"/subjects/{subject}/versions/{version}")
        r.raise_for_status()
    sv = r.json()
    console.print(Panel(
        Syntax(json.dumps(sv["schema_definition"], indent=2), "json", theme="monokai"),
        title=f"{subject} v{sv['version']} [{sv['schema_hash']}]",
    ))


@schemas_app.command("register")
def schemas_register(
    subject: str,
    file: Path = typer.Argument(..., help="Path to JSON Schema file"),
    server: str = typer.Option(DEFAULT_HOST),
):
    """Register a new schema version."""
    if not file.exists():
        fatal(f"File not found: {file}")
    schema_def = json.loads(file.read_text())
    with get_client(server) as c:
        r = c.post(f"/subjects/{subject}/versions", json={"schema_definition": schema_def})
    if r.status_code == 409:
        fatal(r.json().get("detail", "Conflict"))
    r.raise_for_status()
    sv = r.json()
    console.print(f"[green]Registered[/] {subject} v{sv['version']} (hash: {sv['schema_hash']})")


# ── Compatibility ─────────────────────────────────────────────────────────────

compat_app = typer.Typer(help="Check schema compatibility")
app.add_typer(compat_app, name="compat")


@compat_app.command("check")
def compat_check(
    subject: str,
    file: Path = typer.Argument(..., help="Path to candidate JSON Schema"),
    mode: Optional[str] = typer.Option(None, help="Override compatibility mode"),
    server: str = typer.Option(DEFAULT_HOST),
):
    """Check if a schema is compatible with the registered versions."""
    schema_def = json.loads(file.read_text())
    body: dict = {"schema_definition": schema_def}
    if mode:
        body["mode"] = mode
    with get_client(server) as c:
        r = c.post(f"/compatibility/subjects/{subject}/versions", json=body)
        r.raise_for_status()
    result = r.json()
    if result["compatible"]:
        console.print(f"[green]✓ Compatible[/] (mode: {result['mode']})")
    else:
        console.print(f"[red]✗ Incompatible[/] (mode: {result['mode']})")
        for err in result.get("errors", []):
            console.print(f"  [yellow]{err['type']}[/] @ {err['path']}: {err['message']}")
        raise typer.Exit(1)


# ── Config ────────────────────────────────────────────────────────────────────

config_app = typer.Typer(help="Manage subject configuration")
app.add_typer(config_app, name="config")


@config_app.command("get")
def config_get(subject: str, server: str = typer.Option(DEFAULT_HOST)):
    with get_client(server) as c:
        r = c.get(f"/config/{subject}")
        r.raise_for_status()
    console.print(r.json())


@config_app.command("set")
def config_set(
    subject: str,
    compatibility: str = typer.Argument(..., help="BACKWARD | FORWARD | FULL | NONE | *_TRANSITIVE"),
    server: str = typer.Option(DEFAULT_HOST),
):
    """Set compatibility mode for a subject."""
    with get_client(server) as c:
        r = c.put(f"/config/{subject}", json={"compatibility": compatibility})
        r.raise_for_status()
    console.print(f"[green]Updated[/] {subject} → {compatibility}")


# ── Migrations ────────────────────────────────────────────────────────────────

migrate_app = typer.Typer(help="Manage migration scripts")
app.add_typer(migrate_app, name="migrate")


@migrate_app.command("list")
def migrate_list(subject: str, server: str = typer.Option(DEFAULT_HOST)):
    with get_client(server) as c:
        r = c.get(f"/subjects/{subject}/migrations")
        r.raise_for_status()
    scripts = r.json()
    t = Table("From", "To", "Steps", "Auto", "Breaking Changes", title=f"Migrations for {subject}")
    for s in scripts:
        breaking = ", ".join(s.get("breaking_changes", [])) or "-"
        t.add_row(
            str(s["from_version"]),
            str(s["to_version"]),
            str(len(s["steps"])),
            "✓" if s["auto_generated"] else "✗",
            breaking,
        )
    console.print(t)


@migrate_app.command("generate")
def migrate_generate(
    subject: str,
    from_version: int,
    to_version: int,
    server: str = typer.Option(DEFAULT_HOST),
):
    """Auto-generate a migration between two versions."""
    with get_client(server) as c:
        r = c.post(f"/subjects/{subject}/migrations/generate/{from_version}/{to_version}")
        r.raise_for_status()
    script = r.json()
    console.print(f"[green]Generated[/] migration v{from_version} → v{to_version}")
    console.print(Panel(Syntax(script["dsl_source"], "yaml", theme="monokai"), title="DSL"))


@migrate_app.command("upload")
def migrate_upload(
    subject: str,
    from_version: int,
    to_version: int,
    file: Path = typer.Argument(..., help="YAML DSL file"),
    server: str = typer.Option(DEFAULT_HOST),
):
    """Upload a hand-crafted DSL migration."""
    dsl_source = file.read_text()
    with get_client(server) as c:
        r = c.put(
            f"/subjects/{subject}/migrations/{from_version}/{to_version}/dsl",
            json={"dsl_source": dsl_source},
        )
        r.raise_for_status()
    console.print(f"[green]Uploaded[/] migration v{from_version} → v{to_version}")


@migrate_app.command("apply")
def migrate_apply(
    subject: str,
    from_version: int,
    to_version: int,
    payload_file: Path = typer.Argument(..., help="JSON file with payload"),
    server: str = typer.Option(DEFAULT_HOST),
):
    """Apply the migration chain to a single JSON payload."""
    payload = json.loads(payload_file.read_text())
    with get_client(server) as c:
        r = c.post(
            f"/subjects/{subject}/migrate",
            json={"payload": payload, "from_version": from_version, "to_version": to_version},
        )
        r.raise_for_status()
    result = r.json()
    console.print(f"Applied [bold]{result['steps_applied']}[/] migration step(s).")
    console.print(Panel(Syntax(json.dumps(result["migrated"], indent=2), "json", theme="monokai"), title="Result"))


# ── Replay ────────────────────────────────────────────────────────────────────

replay_app = typer.Typer(help="Replay historical events")
app.add_typer(replay_app, name="replay")


@replay_app.command("run")
def replay_run(
    subject: str,
    events_file: Path = typer.Argument(..., help="JSON file with events array"),
    target_version: int = typer.Argument(...),
    no_validate: bool = typer.Option(False, "--no-validate"),
    server: str = typer.Option(DEFAULT_HOST),
):
    """Replay a batch of events through the migration chain to target_version."""
    events = json.loads(events_file.read_text())
    body = {"events": events, "target_version": target_version, "validate": not no_validate}
    with get_client(server) as c:
        r = c.post(f"/subjects/{subject}/replay", json=body)
        r.raise_for_status()
    result = r.json()
    color = "green" if result["failed"] == 0 else "yellow"
    console.print(
        f"[{color}]Replay complete:[/] "
        f"{result['succeeded']}/{result['total']} succeeded, "
        f"{result['failed']} failed."
    )
    if result["errors"]:
        for err in result["errors"]:
            console.print(f"  [red]✗[/] {err['event_id']}: {err['error']}")


if __name__ == "__main__":
    app()
