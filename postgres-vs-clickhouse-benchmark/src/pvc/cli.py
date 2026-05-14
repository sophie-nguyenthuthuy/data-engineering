"""``pvcctl`` command-line interface."""

from __future__ import annotations

import argparse


def cmd_info(_args: argparse.Namespace) -> int:
    from pvc import __version__

    print(f"postgres-vs-clickhouse-benchmark {__version__}")
    return 0


def cmd_list_queries(args: argparse.Namespace) -> int:
    from pvc.workloads.nytaxi import NY_TAXI_QUERIES
    from pvc.workloads.tpch import TPCH_QUERIES

    workload = NY_TAXI_QUERIES if args.workload == "ny-taxi" else TPCH_QUERIES
    for q in workload.queries:
        print(f"{q.id:8s}  {q.description}")
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from pvc.benchmark import BenchmarkRunner
    from pvc.engines.sqlite import SQLiteEngine
    from pvc.report import build_comparison
    from pvc.workloads.base import Query, Workload

    eng = SQLiteEngine()
    eng.setup(
        ddl=["CREATE TABLE numbers (id INTEGER, val INTEGER)"],
        inserts=[
            (
                "INSERT INTO numbers (id, val) VALUES (?, ?)",
                [(i, i * 2) for i in range(args.rows)],
            )
        ],
    )
    workload = Workload(
        name="demo",
        queries=(
            Query(id="count", description="row count", sql="SELECT COUNT(*) FROM numbers"),
            Query(id="sum", description="sum of val", sql="SELECT SUM(val) FROM numbers"),
            Query(
                id="filter",
                description="filtered count",
                sql="SELECT COUNT(*) FROM numbers WHERE val > 10",
            ),
        ),
    )
    runner = BenchmarkRunner(engines=[eng], workload=workload, warmup=1, repeat=5)
    results = runner.run()
    report = build_comparison(results, baseline="sqlite")
    print(f"{'query':<10} {'engine':<10} {'p50_ms':>10} {'p95_ms':>10}")
    for row in report.rows:
        print(
            f"{row.query_id:<10} {row.engine:<10} {row.p50 * 1000:>10.4f} {row.p95 * 1000:>10.4f}"
        )
    eng.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pvcctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)

    lq = sub.add_parser("list-queries", help="show workload queries")
    lq.add_argument("--workload", choices=("tpch", "ny-taxi"), default="tpch")
    lq.set_defaults(func=cmd_list_queries)

    d = sub.add_parser("demo", help="run a tiny SQLite benchmark to verify the harness")
    d.add_argument("--rows", type=int, default=10_000)
    d.set_defaults(func=cmd_demo)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
