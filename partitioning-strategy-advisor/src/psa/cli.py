"""``psactl`` command-line interface."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def cmd_info(_args: argparse.Namespace) -> int:
    from psa import __version__

    print(f"partitioning-strategy-advisor {__version__}")
    return 0


def cmd_profile(args: argparse.Namespace) -> int:
    from psa.profile import Profiler

    raw = Path(args.file).read_text() if args.file else sys.stdin.read()
    queries = [q.strip() for q in raw.split(";") if q.strip()]
    prof = Profiler()
    prof.consume(queries)
    p = prof.build()
    print(f"n_queries = {p.n_queries}")
    print(f"{'column':<20} {'filter':>8} {'join':>6} {'group':>6}")
    for c in sorted(p.columns, key=lambda x: -x.total()):
        print(f"{c.name:<20} {c.filter_count:>8} {c.join_count:>6} {c.group_count:>6}")
    return 0


def cmd_recommend(args: argparse.Namespace) -> int:
    from psa.profile import Profiler
    from psa.recommender import recommend

    raw = Path(args.file).read_text() if args.file else sys.stdin.read()
    queries = [q.strip() for q in raw.split(";") if q.strip()]
    prof = Profiler()
    prof.consume(queries)
    profile = prof.build()
    part, bucket = recommend(profile, target_partitions=args.target_partitions)
    print(f"partition_by = {part.column}   ({part.reason})")
    print(f"bucket_by    = {bucket.column}   buckets={bucket.bucket_count}   ({bucket.reason})")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="psactl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)

    pf = sub.add_parser("profile", help="emit a column-usage profile from a SQL log")
    pf.add_argument("--file", default=None)
    pf.set_defaults(func=cmd_profile)

    rec = sub.add_parser("recommend", help="recommend partition + bucket strategy")
    rec.add_argument("--file", default=None)
    rec.add_argument("--target-partitions", dest="target_partitions", type=int, default=200)
    rec.set_defaults(func=cmd_recommend)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
