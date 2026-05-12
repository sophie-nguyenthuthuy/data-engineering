"""IVM CLI."""

from __future__ import annotations

import argparse
import sys


def cmd_bench(args: argparse.Namespace) -> int:
    if args.what == "delta-vs-full":
        from benchmarks.bench_delta_vs_full import main as bench_main
        bench_main()
    else:
        print(f"unknown bench target: {args.what}", file=sys.stderr)
        return 2
    return 0


def cmd_info(_: argparse.Namespace) -> int:
    from ivm import __version__
    print(f"ivm-nested-aggregates version {__version__}")
    print("IVM for ROW_NUMBER, RANK, DENSE_RANK, LAG/LEAD, sliding SUM/AVG,")
    print("correlated subqueries, nested aggregates (MAX(SUM), SUM(MAX))")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ivmctl", description="IVM CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_bench = sub.add_parser("bench")
    p_bench.add_argument("what", choices=["delta-vs-full"])
    p_bench.set_defaults(func=cmd_bench)
    p_info = sub.add_parser("info")
    p_info.set_defaults(func=cmd_info)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
