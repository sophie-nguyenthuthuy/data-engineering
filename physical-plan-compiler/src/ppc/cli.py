"""Command-line interface.

Examples:
    ppc compile --sql q.sql --catalog tpch.yaml --emit duckdb
    ppc compile --sql q.sql --catalog tpch.yaml --emit dagster -o pipeline.yml
    ppc explain --sql q.sql --catalog tpch.yaml
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ppc.codegen import emit_dagster, emit_dbt, emit_duckdb, emit_flink, emit_spark
from ppc.frontend.catalog import Catalog
from ppc.frontend.sql import sql_to_logical
from ppc.ir.schema import Column, Schema, Stats
from ppc.ir.types import BOOLEAN, DOUBLE, INT32, INT64, STRING, TIMESTAMP

_TYPES = {
    "INT32": INT32, "INT64": INT64, "DOUBLE": DOUBLE,
    "STRING": STRING, "BOOLEAN": BOOLEAN, "TIMESTAMP": TIMESTAMP,
}


def _load_catalog(path: Path) -> Catalog:
    """Catalog file format (YAML or JSON):

    tables:
      orders:
        rows: 1500000
        columns:
          - {name: o_orderkey, type: INT64, ndv: 1500000}
          - {name: o_totalprice, type: DOUBLE}
    """
    raw = path.read_text()
    if path.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml
            data = yaml.safe_load(raw)
        except ImportError:
            print("YAML catalog requires PyYAML (pip install pyyaml)", file=sys.stderr)
            sys.exit(2)
    else:
        data = json.loads(raw)
    cat = Catalog()
    for table_name, tspec in data.get("tables", {}).items():
        cols = []
        for c in tspec["columns"]:
            dtype = _TYPES[c["type"]]
            stats = Stats(ndv=c.get("ndv"), nulls=c.get("nulls", 0.0),
                          avg_len=c.get("avg_len"))
            cols.append(Column(name=c["name"], dtype=dtype, stats=stats))
        cat.register(table_name, Schema.of(*cols, rows=tspec.get("rows")))
    return cat


EMITTERS = {
    "spark": emit_spark,
    "dbt": emit_dbt,
    "duckdb": emit_duckdb,
    "flink": emit_flink,
}


def cmd_compile(args: argparse.Namespace) -> int:
    sql = Path(args.sql).read_text() if args.sql.endswith(".sql") else args.sql
    catalog = _load_catalog(Path(args.catalog))
    from ppc.cascades.optimizer import Optimizer
    logical = sql_to_logical(sql, catalog)
    plan = Optimizer(catalog=catalog).optimize(logical)

    out: str
    if args.emit == "dagster":
        manifest = emit_dagster(plan)
        try:
            import yaml
            out = yaml.safe_dump(manifest, sort_keys=False)
        except ImportError:
            out = json.dumps(manifest, indent=2)
    elif args.emit in EMITTERS:
        out = EMITTERS[args.emit](plan)
    else:
        print(f"unknown --emit target: {args.emit}", file=sys.stderr)
        return 2

    if args.output:
        Path(args.output).write_text(out)
        print(f"Wrote {args.output} (cost={plan.total_cost:.2f}, engine={plan.root.engine})",
              file=sys.stderr)
    else:
        sys.stdout.write(out)
    return 0


def cmd_explain(args: argparse.Namespace) -> int:
    sql = Path(args.sql).read_text() if args.sql.endswith(".sql") else args.sql
    catalog = _load_catalog(Path(args.catalog))
    from ppc.cascades.optimizer import Optimizer
    logical = sql_to_logical(sql, catalog)
    plan = Optimizer(catalog=catalog).optimize(logical)

    try:
        from rich.console import Console
        from rich.tree import Tree
    except ImportError:
        print("--- Logical ---")
        print(logical.explain())
        print(f"--- Physical (cost={plan.total_cost:.2f}) ---")
        print(plan.explain())
        return 0
    console = Console()
    console.rule("[bold]Logical plan")
    console.print(logical.explain())
    console.rule(f"[bold]Physical plan (cost={plan.total_cost:.2f})")
    console.print(plan.explain())
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ppc", description="Physical Plan Compiler")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_compile = sub.add_parser("compile", help="Compile SQL → engine artefact")
    p_compile.add_argument("--sql", required=True, help="SQL string or path to .sql file")
    p_compile.add_argument("--catalog", required=True, help="Path to catalog .yaml/.json")
    p_compile.add_argument("--emit", required=True,
                           choices=["spark", "dbt", "duckdb", "flink", "dagster"])
    p_compile.add_argument("--output", "-o", help="Output file (default: stdout)")
    p_compile.set_defaults(func=cmd_compile)

    p_explain = sub.add_parser("explain", help="Show optimization choices")
    p_explain.add_argument("--sql", required=True)
    p_explain.add_argument("--catalog", required=True)
    p_explain.set_defaults(func=cmd_explain)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
