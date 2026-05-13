"""``acectl`` command-line interface."""

from __future__ import annotations

import argparse


def cmd_info(_args: argparse.Namespace) -> int:
    from ace import __version__

    print(f"adversarial-chaos-engine {__version__}")
    return 0


def cmd_edges(_args: argparse.Namespace) -> int:
    from ace.edges.numeric import numeric_edges
    from ace.edges.strings import string_edges
    from ace.edges.timestamps import timestamp_edges

    print(f"numeric_edges   : {len(numeric_edges())} values")
    print(f"string_edges    : {len(string_edges())} values")
    print(f"timestamp_edges : {len(timestamp_edges())} values")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    from ace.invariants.catalog import Catalog
    from ace.runner import Runner

    cat = Catalog()

    # Demo bug: abs() over `amount` violates sum_invariant.
    @cat.invariant(sum_invariant=["amount"])
    def buggy_abs(frame: list[dict[str, object]]) -> list[dict[str, object]]:
        out: list[dict[str, object]] = []
        for r in frame:
            v = r.get("amount")
            if isinstance(v, int | float):
                out.append({**r, "amount": abs(v)})
            else:
                out.append(r)
        return out

    r = Runner(catalog=cat, seed=args.seed)
    report = r.run(trials=args.trials)
    print(f"trials={report.n_trials}  pipelines={report.n_pipelines}")
    print(f"failing={len(report.failing())}  exceptions={len(report.exceptions())}")
    for v in report.failing():
        print(f"  {v.fn_name} → {v.invariant}  (input rows: {len(v.input)})")
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    from ace.bench import run_benchmark

    rep = run_benchmark(trials=args.trials, seed=args.seed)
    print(f"trials per pipeline = {rep.n_trials}")
    print(f"targeted bugs found = {rep.targeted_bugs}")
    print(f"random   bugs found = {rep.random_bugs}")
    speedup_str = "∞" if rep.speedup == float("inf") else f"{rep.speedup:.2f}x"
    print(f"speedup             = {speedup_str}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="acectl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)
    sub.add_parser("edges", help="count entries in each edge-case library").set_defaults(
        func=cmd_edges
    )

    r = sub.add_parser("run", help="run a demo buggy pipeline")
    r.add_argument("--trials", type=int, default=100)
    r.add_argument("--seed", type=int, default=0)
    r.set_defaults(func=cmd_run)

    b = sub.add_parser("bench", help="compare targeted vs. random fuzzing on the bug zoo")
    b.add_argument("--trials", type=int, default=100)
    b.add_argument("--seed", type=int, default=0)
    b.set_defaults(func=cmd_bench)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
