"""``fpsctl`` command-line interface."""

from __future__ import annotations

import argparse


def cmd_info(_args: argparse.Namespace) -> int:
    from fps import __version__

    print(f"forecasting-pipeline-scheduler {__version__}")
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    from fps.bench import random_layered_dag
    from fps.scheduler.baseline import baseline_fcfs_schedule
    from fps.scheduler.branch_bound import branch_and_bound
    from fps.scheduler.common import makespan
    from fps.scheduler.list_sched import list_schedule

    dag = random_layered_dag(
        n_layers=args.layers,
        avg_layer_width=args.width,
        seed=args.seed,
    )
    base = baseline_fcfs_schedule(dag, args.workers)
    cps = list_schedule(dag, args.workers)
    print(f"tasks={len(dag)}  workers={args.workers}  seed={args.seed}")
    print(f"  baseline_makespan      = {makespan(base):.4f}")
    print(f"  list_schedule_makespan = {makespan(cps):.4f}")
    if len(dag) <= 12:
        bb = branch_and_bound(dag, args.workers, time_limit_ms=args.time_limit_ms)
        print(f"  branch_and_bound       = {makespan(bb):.4f}")
    return 0


def cmd_regret(args: argparse.Namespace) -> int:
    from fps.bench import random_layered_dag
    from fps.shadow import regret_over_dags

    dags = [
        random_layered_dag(
            n_layers=args.layers,
            avg_layer_width=args.width,
            seed=args.seed + i,
        )
        for i in range(args.n_dags)
    ]
    agg = regret_over_dags(dags, num_workers=args.workers)
    print(
        f"n_dags={agg.n_dags}  workers={args.workers}  "
        f"mean_regret={agg.mean_regret:+.4f}  median_regret={agg.median_regret:+.4f}"
    )
    print(
        f"p95_regret={agg.p95_regret:+.4f}  "
        f"mean_speedup={agg.mean_speedup:.4f}  "
        f"positive_fraction={agg.positive_fraction():.3f}"
    )
    return 0


def cmd_forecast(args: argparse.Namespace) -> int:
    import math
    import random

    from fps.forecast.lognormal import LognormalForecaster

    rng = random.Random(args.seed)
    f = LognormalForecaster()
    for _ in range(args.samples):
        f.observe("task", math.exp(args.mu + args.sigma * rng.gauss(0.0, 1.0)))
    print(f"samples={args.samples}  true_mu={args.mu}  true_sigma={args.sigma}")
    print(f"  fit_mu     = {f.stats('task').mu:.4f}")
    print(f"  fit_sigma  = {f.stats('task').sigma:.4f}")
    print(f"  mean_est   = {f.mean('task'):.4f}")
    print(f"  p95_est    = {f.p95('task'):.4f}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="fpsctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)

    b = sub.add_parser("bench", help="run all three schedulers on a synthetic DAG")
    b.add_argument("--layers", type=int, default=4)
    b.add_argument("--width", type=int, default=3)
    b.add_argument("--workers", type=int, default=2)
    b.add_argument("--seed", type=int, default=0)
    b.add_argument("--time-limit-ms", dest="time_limit_ms", type=int, default=200)
    b.set_defaults(func=cmd_bench)

    r = sub.add_parser("regret", help="aggregate regret over many synthetic DAGs")
    r.add_argument("--n-dags", dest="n_dags", type=int, default=100)
    r.add_argument("--layers", type=int, default=5)
    r.add_argument("--width", type=int, default=4)
    r.add_argument("--workers", type=int, default=2)
    r.add_argument("--seed", type=int, default=0)
    r.set_defaults(func=cmd_regret)

    f = sub.add_parser("forecast", help="fit lognormal forecaster to a synthetic stream")
    f.add_argument("--samples", type=int, default=500)
    f.add_argument("--mu", type=float, default=2.0)
    f.add_argument("--sigma", type=float, default=0.5)
    f.add_argument("--seed", type=int, default=0)
    f.set_defaults(func=cmd_forecast)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
