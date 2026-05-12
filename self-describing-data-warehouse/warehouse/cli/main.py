"""
CLI for the Self-Describing Data Warehouse.

Usage examples:
  sdw describe orders
  sdw recommend "monthly revenue by region"
  sdw catalog
  sdw catalog --domain finance
  sdw health
  sdw lineage orders --direction downstream
  sdw quality orders --run
  sdw incidents list
"""

import argparse
import json
import sys
from pathlib import Path


def get_warehouse(db_path: str):
    from warehouse.core.warehouse import SelfDescribingWarehouse
    return SelfDescribingWarehouse(db_path=db_path)


# ------------------------------------------------------------------ #
#  Formatters                                                          #
# ------------------------------------------------------------------ #

def _print_json(data):
    print(json.dumps(data, indent=2, default=str))


def _print_table(rows: list[dict], cols: list[str] | None = None):
    if not rows:
        print("  (no results)")
        return
    cols = cols or list(rows[0].keys())
    widths = {c: max(len(c), max(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    header = "  " + "  ".join(c.ljust(widths[c]) for c in cols)
    sep = "  " + "  ".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for row in rows:
        print("  " + "  ".join(str(row.get(c, "")).ljust(widths[c]) for c in cols))


# ------------------------------------------------------------------ #
#  Commands                                                            #
# ------------------------------------------------------------------ #

def cmd_describe(args, wh):
    data = wh.describe(args.table)
    if "error" in data:
        print(f"Error: {data['error']}", file=sys.stderr)
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  TABLE: {data['table_name']}")
    if data.get("is_deprecated"):
        print(f"  *** DEPRECATED: {data.get('deprecation_note', '')} ***")
    print(f"{'='*60}")
    print(f"  Description   : {data['description']}")
    print(f"  Owner         : {data['owner']}")
    print(f"  Domain        : {data['domain']}")
    print(f"  Source        : {data.get('source_system', '')}")
    print(f"  Refresh       : {data.get('update_frequency', '')}")
    print(f"  Tags          : {', '.join(data.get('tags', []))}")

    print(f"\n  Columns ({len(data.get('columns', []))}):")
    for col in data.get("columns", []):
        pii = " [PII]" if col.get("is_pii") else ""
        null = " nullable" if col.get("is_nullable") else " NOT NULL"
        samples = ", ".join(str(v) for v in (col.get("sample_values") or [])[:3])
        samples_str = f"  e.g. {samples}" if samples else ""
        print(f"    • {col['column_name']} ({col['data_type']}){pii}{null}{samples_str}")
        if col.get("description"):
            print(f"        {col['description']}")

    q = data.get("quality")
    if q:
        print(f"\n  Quality (score {q['quality_score']}/100):")
        print(f"    Rows: {q['row_count']}  Null rate: {q['null_rate']:.1%}  "
              f"Dup rate: {q['duplicate_rate']:.1%}  Violations: {q['constraint_violations']}")

    f = data.get("freshness")
    if f:
        print(f"\n  Freshness (score {f['freshness_score']}/100):")
        print(f"    Last updated: {f['last_updated_at']}  "
              f"({f['hours_since_update']:.1f}h ago, expected every {f['expected_interval_hours']}h)")

    u = data.get("usage")
    if u:
        print(f"\n  Usage:")
        print(f"    Total queries: {u.get('total_queries', 0)}  "
              f"Unique users: {u.get('unique_users', 0)}  "
              f"Last queried: {u.get('last_queried_at', 'never')}")

    top_users = data.get("top_users", [])
    if top_users:
        print(f"    Top users: " + ", ".join(f"{u['queried_by']}({u['query_count']})" for u in top_users))

    lin = data.get("lineage", {})
    up = lin.get("upstream", [])
    dn = lin.get("downstream", [])
    if up or dn:
        print(f"\n  Lineage:")
        if up:
            print(f"    Upstream  : {', '.join(t['table_name'] for t in up)}")
        if dn:
            print(f"    Downstream: {', '.join(t['table_name'] for t in dn)}")

    last_inc = data.get("last_incident")
    open_inc = data.get("open_incidents", [])
    if last_inc or open_inc:
        print(f"\n  Incidents:")
        if open_inc:
            print(f"    ⚠ {len(open_inc)} open incident(s)!")
        if last_inc:
            print(f"    Last incident: [{last_inc['severity']}] {last_inc['description']} "
                  f"({last_inc['occurred_at'][:10]})")
    print()


def cmd_recommend(args, wh):
    results = wh.recommend(args.query, domain=args.domain, top_k=args.top)
    print(f"\n  Recommendations for: \"{args.query}\"\n")
    if not results:
        print("  No tables matched.")
        return
    for i, r in enumerate(results, 1):
        print(f"  #{i} (score {r.composite_score}/100)")
        print(str(r))


def cmd_catalog(args, wh):
    tables = wh.catalog(domain=args.domain)
    if not tables:
        print("  No tables registered.")
        return
    print(f"\n  Catalog ({len(tables)} tables):\n")
    _print_table(
        tables,
        cols=["table_name", "domain", "owner", "update_frequency", "source_system"],
    )
    print()


def cmd_health(args, wh):
    rows = wh.health_dashboard()
    print(f"\n  Health Dashboard ({len(rows)} tables):\n")
    _print_table(
        rows,
        cols=["table_name", "domain", "quality_score", "freshness_score",
              "total_queries", "open_incidents", "trend"],
    )
    print()


def cmd_lineage(args, wh):
    if args.direction in ("up", "upstream"):
        result = wh.lineage.upstream(args.table)
        label = "Upstream"
    elif args.direction in ("down", "downstream"):
        result = wh.lineage.downstream(args.table)
        label = "Downstream"
    else:
        impact = wh.lineage.impact_analysis(args.table)
        print(f"\n  Impact analysis for {args.table}:")
        print(f"  {impact['total_affected']} table(s) would be affected.\n")
        if impact["affected_tables"]:
            _print_table(impact["affected_tables"], cols=["table_name", "depth"])
        print()
        return
    print(f"\n  {label} lineage for {args.table}:\n")
    if result:
        _print_table(result, cols=["table_name", "depth"])
    else:
        print("  (none)")
    print()


def cmd_quality(args, wh):
    if args.run:
        result = wh.quality.run(args.table)
        print(f"\n  Quality run for {args.table}:")
        _print_json(result)
    history = wh.quality.history(args.table, limit=5)
    print(f"\n  Quality history for {args.table} (last {len(history)}):\n")
    _print_table(history, cols=["run_at", "row_count", "null_rate", "duplicate_rate", "quality_score"])
    print(f"  Trend: {wh.quality.trend(args.table)}\n")


def cmd_incidents(args, wh):
    if args.action == "list":
        incidents = wh.incidents.open_incidents(args.table or None)
        print(f"\n  Open incidents{' for ' + args.table if args.table else ''}:\n")
        if incidents:
            _print_table(
                incidents,
                cols=["id", "table_name", "severity", "description", "occurred_at"],
            )
        else:
            print("  No open incidents.")
        print()
    elif args.action == "history":
        if not args.table:
            print("Error: --table required for history", file=sys.stderr)
            sys.exit(1)
        history = wh.incidents.history(args.table)
        _print_table(
            history,
            cols=["id", "severity", "description", "occurred_at", "resolved_at"],
        )


# ------------------------------------------------------------------ #
#  Entry point                                                         #
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(
        prog="sdw",
        description="Self-Describing Data Warehouse CLI",
    )
    parser.add_argument(
        "--db", default="warehouse.db",
        help="Path to SQLite database (default: warehouse.db)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # describe
    p = sub.add_parser("describe", help="Show all metadata for a table")
    p.add_argument("table")

    # recommend
    p = sub.add_parser("recommend", help="Find the best table for a query")
    p.add_argument("query")
    p.add_argument("--domain", default=None)
    p.add_argument("--top", type=int, default=5)

    # catalog
    p = sub.add_parser("catalog", help="List all registered tables")
    p.add_argument("--domain", default=None)

    # health
    sub.add_parser("health", help="Show quality/freshness dashboard for all tables")

    # lineage
    p = sub.add_parser("lineage", help="Show table lineage")
    p.add_argument("table")
    p.add_argument("--direction", default="impact",
                   choices=["upstream", "up", "downstream", "down", "impact"])

    # quality
    p = sub.add_parser("quality", help="Show/run quality checks for a table")
    p.add_argument("table")
    p.add_argument("--run", action="store_true", help="Run a fresh quality check")

    # incidents
    p = sub.add_parser("incidents", help="View incident history")
    p.add_argument("action", choices=["list", "history"])
    p.add_argument("--table", default=None)

    args = parser.parse_args()
    wh = get_warehouse(args.db)

    dispatch = {
        "describe":  cmd_describe,
        "recommend": cmd_recommend,
        "catalog":   cmd_catalog,
        "health":    cmd_health,
        "lineage":   cmd_lineage,
        "quality":   cmd_quality,
        "incidents": cmd_incidents,
    }
    dispatch[args.command](args, wh)
    wh.close()


if __name__ == "__main__":
    main()
