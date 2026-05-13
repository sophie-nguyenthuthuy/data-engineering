"""``llo`` command-line interface."""

from __future__ import annotations

import argparse


def cmd_info(_args: argparse.Namespace) -> int:
    from llo import __version__

    print(f"learned-layout-optimizer {__version__}")
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    from llo.bench import evaluate_static, make_dataset, make_shifted_workload
    from llo.policy.bandit import Action

    data, cols = make_dataset(n_rows=args.rows, seed=args.seed)
    wl = make_shifted_workload(cols, n_queries=args.queries, shift_every=args.shift_every)
    actions = [
        ("noop", Action("noop", ())),
        ("sortkey:a", Action("sortkey", ("a",))),
        ("zorder:a,b", Action("zorder", ("a", "b"))),
        ("hilbert:a,b", Action("hilbert", ("a", "b"))),
        ("zorder:a,b,c,d", Action("zorder", tuple(cols))),
    ]
    print(f"rows={args.rows}  queries={args.queries}  shift_every={args.shift_every}")
    print(f"{'layout':<18} {'mean pages':>12}")
    for label, a in actions:
        r = evaluate_static(data, cols, a, wl, label)
        print(f"{label:<18} {r.mean_pages:>12.2f}")
    return 0


def cmd_simulate(args: argparse.Namespace) -> int:
    from llo.agent.loop import LayoutAgent
    from llo.bench import make_dataset, make_shifted_workload
    from llo.policy.bandit import Action, UCBPolicy
    from llo.workload.profile import WorkloadProfile

    data, cols = make_dataset(n_rows=args.rows, seed=args.seed)
    wl = make_shifted_workload(cols, n_queries=args.queries, shift_every=args.shift_every)
    profile = WorkloadProfile(columns=cols)
    actions = [
        Action("noop", ()),
        Action("sortkey", ("a",)),
        Action("zorder", ("a", "b")),
        Action("hilbert", ("a", "b")),
        Action("zorder", tuple(cols)),
    ]
    agent = LayoutAgent(data=data, columns=cols, policy=UCBPolicy(actions=actions), profile=profile)
    history = agent.run(wl, act_every=args.act_every)
    print(f"steps={len(history)}  final action={history[-1].action}")
    print(f"{'step':>4} {'action':<22} {'reward':>10} {'drift':>8}")
    for log in history[-10:]:
        print(f"{log.step:>4} {log.action!r:<22} {log.reward:>10.3f} {log.drift:>8.3f}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="llo")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)

    b = sub.add_parser("bench", help="evaluate static layouts on a shifted workload")
    b.add_argument("--rows", type=int, default=4096)
    b.add_argument("--queries", type=int, default=800)
    b.add_argument("--shift-every", dest="shift_every", type=int, default=200)
    b.add_argument("--seed", type=int, default=0)
    b.set_defaults(func=cmd_bench)

    s = sub.add_parser("simulate", help="run the closed-loop agent")
    s.add_argument("--rows", type=int, default=4096)
    s.add_argument("--queries", type=int, default=800)
    s.add_argument("--shift-every", dest="shift_every", type=int, default=200)
    s.add_argument("--act-every", dest="act_every", type=int, default=50)
    s.add_argument("--seed", type=int, default=0)
    s.set_defaults(func=cmd_simulate)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
