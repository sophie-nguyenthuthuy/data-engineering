"""``tflctl`` command-line interface."""

from __future__ import annotations

import argparse
import random


def cmd_info(_args: argparse.Namespace) -> int:
    from tfl import __version__

    print(f"delta-vs-iceberg-vs-hudi {__version__}")
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    from tfl.bench.compare import run_workload
    from tfl.bench.workload import CDCEvent, CDCOp, Workload

    rng = random.Random(args.seed)
    events: list[CDCEvent] = []
    keys = [f"k{i:04d}" for i in range(args.distinct_keys)]
    for _ in range(args.events):
        op = rng.choices(
            [CDCOp.INSERT, CDCOp.UPDATE, CDCOp.DELETE],
            weights=[args.insert_pct, args.update_pct, args.delete_pct],
        )[0]
        events.append(CDCEvent(op=op, key=rng.choice(keys), payload_size=128))
    wl = Workload(name="synthetic-cdc", events=tuple(events))
    report = run_workload(wl)
    print(f"workload    = {report.workload}")
    print(f"update_pct  = {wl.update_ratio():.2f}")
    print(f"{'format':<10} {'commits':>10} {'write_amp':>10} {'read_files':>11}")
    for m in report.metrics:
        print(f"{m.name:<10} {m.commits:>10} {m.write_amplification:>10} {m.read_files_at_end:>11}")
    print(f"lowest_write_amp = {report.lowest_write_amplification()}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tflctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)
    c = sub.add_parser("compare", help="run a synthetic CDC workload on all four formats")
    c.add_argument("--events", type=int, default=500)
    c.add_argument("--distinct-keys", dest="distinct_keys", type=int, default=50)
    c.add_argument("--insert-pct", dest="insert_pct", type=int, default=20)
    c.add_argument("--update-pct", dest="update_pct", type=int, default=70)
    c.add_argument("--delete-pct", dest="delete_pct", type=int, default=10)
    c.add_argument("--seed", type=int, default=0)
    c.set_defaults(func=cmd_compare)
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
