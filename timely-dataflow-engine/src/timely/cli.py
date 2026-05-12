"""CLI."""

from __future__ import annotations

import argparse
import sys


def cmd_bench(args: argparse.Namespace) -> int:
    if args.what == "pagerank":
        from benchmarks.bench_pagerank import main as bench_main
        bench_main()
    else:
        print(f"unknown bench: {args.what}", file=sys.stderr)
        return 2
    return 0


def cmd_info(_: argparse.Namespace) -> int:
    from timely import __version__
    print(f"timely-dataflow-engine version {__version__}")
    print("Naiad-style (epoch, iteration) timestamps + progress tracking")
    return 0


def cmd_pagerank(args: argparse.Namespace) -> int:
    from timely.examples.pagerank import pagerank
    # Simple cycle for demonstration
    edges = {0: [1], 1: [2], 2: [0]}
    ranks, iters = pagerank(edges, n_nodes=3, max_iter=args.max_iter)
    print(f"Converged in {iters} iterations.")
    for i, r in enumerate(ranks):
        print(f"  PR[{i}] = {r:.6f}")
    print(f"  Sum    = {sum(ranks):.6f}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="timelyctl", description="Timely Dataflow CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_bench = sub.add_parser("bench")
    p_bench.add_argument("what", choices=["pagerank"])
    p_bench.set_defaults(func=cmd_bench)
    p_info = sub.add_parser("info")
    p_info.set_defaults(func=cmd_info)
    p_pr = sub.add_parser("pagerank", help="Run a small PageRank example")
    p_pr.add_argument("--max-iter", type=int, default=100)
    p_pr.set_defaults(func=cmd_pagerank)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
