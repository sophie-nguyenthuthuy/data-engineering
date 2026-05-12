"""Command-line interface for the disaggregated storage engine.

Examples:
    disaggctl bench lookup
    disaggctl bench prefetch
    disaggctl info
"""

from __future__ import annotations

import argparse
import sys


def cmd_bench(args: argparse.Namespace) -> int:
    if args.what == "lookup":
        from benchmarks.bench_lookup import main as bench_main

        bench_main()
    elif args.what == "prefetch":
        from benchmarks.bench_prefetch import main as bench_main

        bench_main()
    else:
        print(f"unknown bench target: {args.what}", file=sys.stderr)
        return 2
    return 0


def cmd_info(_: argparse.Namespace) -> int:
    from disagg import __version__

    print(f"disaggregated-storage-engine version {__version__}")
    print("Engine: remote page server + per-client cache + Markov prefetcher")
    print("Coherence: sharded directory with write-invalidate")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="disaggctl",
                                     description="Disaggregated storage engine")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_bench = sub.add_parser("bench", help="Run benchmarks")
    p_bench.add_argument("what", choices=["lookup", "prefetch"])
    p_bench.set_defaults(func=cmd_bench)

    p_info = sub.add_parser("info", help="Show version & engine info")
    p_info.set_defaults(func=cmd_info)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
