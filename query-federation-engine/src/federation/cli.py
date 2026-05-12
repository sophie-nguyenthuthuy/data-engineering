"""
Command-line interface for the Query Federation Engine.

Usage
-----
    # Run a query from an argument
    qfe query --config config/catalog.yaml "SELECT * FROM postgres.orders LIMIT 5"

    # Run a query from a file
    qfe query --config config/catalog.yaml --file my_query.sql

    # Explain (show plan without executing)
    qfe explain --config config/catalog.yaml "SELECT u.name, o.total FROM postgres.orders o JOIN mongodb.users u ON o.user_id = u.id"

    # List registered tables
    qfe tables --config config/catalog.yaml
"""

from __future__ import annotations

import argparse
import json
import sys
import textwrap
from pathlib import Path


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qfe",
        description="Query Federation Engine — run SQL across Postgres, MongoDB, S3 Parquet, and REST APIs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              qfe query --config catalog.yaml "SELECT * FROM postgres.orders LIMIT 10"
              qfe explain --config catalog.yaml "SELECT u.name FROM mongodb.users u WHERE u.country = 'US'"
              qfe tables --config catalog.yaml
        """),
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ── query ───────────────────────────────────────────────────────────
    q = sub.add_parser("query", help="Execute a federated SQL query")
    q.add_argument("sql", nargs="?", help="SQL string (use --file for file input)")
    q.add_argument("--file", "-f", help="Path to a .sql file")
    q.add_argument("--config", "-c", required=True, help="Path to catalog YAML")
    q.add_argument(
        "--format", choices=["table", "csv", "json"], default="table",
        help="Output format (default: table)",
    )
    q.add_argument("--stats", action="store_true", help="Print execution stats")
    q.add_argument("--no-color", action="store_true", help="Disable rich formatting")

    # ── explain ─────────────────────────────────────────────────────────
    e = sub.add_parser("explain", help="Show the query plan without executing")
    e.add_argument("sql", nargs="?")
    e.add_argument("--file", "-f")
    e.add_argument("--config", "-c", required=True)

    # ── tables ──────────────────────────────────────────────────────────
    t = sub.add_parser("tables", help="List all registered tables")
    t.add_argument("--config", "-c", required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _make_parser()
    args = parser.parse_args(argv)

    if args.command in ("query", "explain"):
        sql = _read_sql(args)
        if sql is None:
            parser.error("Provide SQL via argument or --file")

    if args.command == "tables":
        return _cmd_tables(args)
    elif args.command == "explain":
        return _cmd_explain(args, sql)  # type: ignore[arg-type]
    elif args.command == "query":
        return _cmd_query(args, sql)  # type: ignore[arg-type]
    return 0


def _read_sql(args: argparse.Namespace) -> str | None:
    if getattr(args, "file", None):
        return Path(args.file).read_text().strip()
    return getattr(args, "sql", None)


# ──────────────────────────────────────────────────────────────────────────────
# Sub-commands
# ──────────────────────────────────────────────────────────────────────────────

def _cmd_tables(args: argparse.Namespace) -> int:
    from .catalog import SchemaCatalog
    try:
        catalog = SchemaCatalog.from_yaml(args.config)
    except Exception as exc:
        _err(f"Failed to load catalog: {exc}")
        return 1

    try:
        from rich.console import Console
        from rich.table import Table as RichTable

        console = Console()
        tbl = RichTable(title="Registered Tables", show_lines=True)
        tbl.add_column("Qualified Name", style="cyan bold")
        tbl.add_column("Source Type", style="yellow")
        tbl.add_column("Columns")
        tbl.add_column("Est. Rows", justify="right")

        for name in sorted(catalog.list_tables()):
            schema = catalog.get_table(name)
            cols = ", ".join(schema.column_names())
            tbl.add_row(name, schema.source_type.value, cols, f"{schema.estimated_rows:,}")

        console.print(tbl)
    except ImportError:
        for name in sorted(catalog.list_tables()):
            schema = catalog.get_table(name)
            print(f"{name}  [{schema.source_type.value}]  rows~{schema.estimated_rows:,}")

    return 0


def _cmd_explain(args: argparse.Namespace, sql: str) -> int:
    from .engine import FederationEngine
    try:
        engine = FederationEngine.from_yaml(args.config)
        plan_text = engine.explain(sql)
    except Exception as exc:
        _err(f"Plan error: {exc}")
        return 1

    try:
        from rich.console import Console
        from rich.syntax import Syntax
        from rich.panel import Panel

        console = Console()
        console.print(Panel(sql, title="SQL", border_style="blue"))
        console.print(Panel(plan_text, title="Query Plan", border_style="green"))
    except ImportError:
        print("=== SQL ===")
        print(sql)
        print("\n=== Query Plan ===")
        print(plan_text)

    return 0


def _cmd_query(args: argparse.Namespace, sql: str) -> int:
    from .engine import FederationEngine

    try:
        engine = FederationEngine.from_yaml(args.config)
        df, stats = engine.query(sql)
    except Exception as exc:
        _err(f"Query error: {exc}")
        return 1

    fmt = getattr(args, "format", "table")
    use_color = not getattr(args, "no_color", False)

    if fmt == "csv":
        print(df.to_csv(index=False), end="")
    elif fmt == "json":
        print(df.to_json(orient="records", indent=2))
    else:
        _print_table(df, use_color)

    if getattr(args, "stats", False):
        _print_stats(stats, use_color)

    return 0


# ──────────────────────────────────────────────────────────────────────────────
# Output helpers
# ──────────────────────────────────────────────────────────────────────────────

def _print_table(df, use_color: bool) -> None:
    if use_color:
        try:
            from rich.console import Console
            from rich.table import Table as RichTable

            console = Console()
            tbl = RichTable(show_lines=False, header_style="bold magenta")
            for col in df.columns:
                tbl.add_column(str(col))
            for _, row in df.iterrows():
                tbl.add_row(*[str(v) for v in row])
            console.print(tbl)
            return
        except ImportError:
            pass
    # Fallback: plain pandas string
    print(df.to_string(index=False))


def _print_stats(stats, use_color: bool) -> None:
    if use_color:
        try:
            from rich.console import Console
            from rich.panel import Panel

            Console().print(Panel(stats.summary(), title="Execution Stats", border_style="dim"))
            return
        except ImportError:
            pass
    print("\n--- Execution Stats ---")
    print(stats.summary())


def _err(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
