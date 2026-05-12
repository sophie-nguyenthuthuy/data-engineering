"""Command-line interface.

Examples:
    artctl bench lookup --n 100000
    artctl bench concurrent --readers 4 --writers 4 --duration 5
    artctl info
"""

from __future__ import annotations

import argparse
import sys


def cmd_bench(args: argparse.Namespace) -> int:
    if args.what == "lookup":
        from benchmarks.bench_lookup import main as bench_main

        bench_main()
    elif args.what == "concurrent":
        from benchmarks.bench_concurrent import workload

        r = workload(
            n_readers=args.readers,
            n_writers=args.writers,
            n_keys=args.keys,
            duration_s=args.duration,
        )
        print(f"reads/s={r['read_qps']:,.0f}  writes/s={r['write_qps']:,.0f}  "
              f"conflict%={r['conflict_rate']*100:.2f}")
    else:
        print(f"unknown bench target: {args.what}", file=sys.stderr)
        return 2
    return 0


def cmd_info(_: argparse.Namespace) -> int:
    from art_mvcc import __version__

    print(f"art-mvcc-index version {__version__}")
    print("Engine: Adaptive Radix Tree + MVCC (snapshot isolation, epoch reclamation)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="artctl", description="ART + MVCC tools")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_bench = sub.add_parser("bench", help="Run benchmarks")
    p_bench.add_argument("what", choices=["lookup", "concurrent"])
    p_bench.add_argument("--readers", type=int, default=4)
    p_bench.add_argument("--writers", type=int, default=4)
    p_bench.add_argument("--keys", type=int, default=128)
    p_bench.add_argument("--duration", type=float, default=2.0)
    p_bench.set_defaults(func=cmd_bench)

    p_info = sub.add_parser("info", help="Show version and config")
    p_info.set_defaults(func=cmd_info)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
