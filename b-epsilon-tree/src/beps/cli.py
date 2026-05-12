"""B^epsilon-tree CLI.

Examples:
    bepsctl bench write
    bepsctl bench read
    bepsctl info
"""

from __future__ import annotations

import argparse
import sys


def cmd_bench(args: argparse.Namespace) -> int:
    if args.what == "write":
        from benchmarks.bench_write import main as bench_main

        bench_main()
    elif args.what == "read":
        from benchmarks.bench_read import main as bench_main

        bench_main()
    else:
        print(f"unknown bench target: {args.what}", file=sys.stderr)
        return 2
    return 0


def cmd_info(_: argparse.Namespace) -> int:
    from beps import __version__

    print(f"b-epsilon-tree version {__version__}")
    print("Engine: write-optimized B^epsilon-tree with online epsilon tuning")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bepsctl", description="B^epsilon-tree CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_bench = sub.add_parser("bench", help="Run benchmarks")
    p_bench.add_argument("what", choices=["write", "read"])
    p_bench.set_defaults(func=cmd_bench)

    p_info = sub.add_parser("info", help="Show version & engine info")
    p_info.set_defaults(func=cmd_info)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
