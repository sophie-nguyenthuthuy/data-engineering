"""``aqpctl`` command-line interface."""

from __future__ import annotations

import argparse


def cmd_info(_args: argparse.Namespace) -> int:
    from aqp import __version__

    print(f"aqp-coreset-engine {__version__}")
    return 0


def cmd_size(args: argparse.Namespace) -> int:
    from aqp.bounds.size import coreset_size_sum, hoeffding_count_size

    m_sum = coreset_size_sum(args.eps, args.delta, vc=args.vc)
    m_cnt = hoeffding_count_size(args.eps, args.delta)
    print(f"ε={args.eps}  δ={args.delta}  vc={args.vc}")
    print(f"  coreset_size_sum     = {m_sum}")
    print(f"  hoeffding_count_size = {m_cnt}")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    import numpy as np

    from aqp.coreset.sensitivity import SensitivityCoreset
    from aqp.coreset.uniform import UniformCoreset
    from aqp.eval import validate_coverage

    rng = np.random.default_rng(args.seed)
    # Synthetic skewed payload: value correlates with payload[0] strongly,
    # so sensitivity sampling should beat uniform on rare-stratum queries.
    n = args.rows
    cat = rng.integers(0, 4, size=n)
    value = (1.0 + 9.0 * (cat == 3)) * rng.uniform(0.5, 1.5, size=n)
    rows: list[tuple[float, tuple[float, ...]]] = [
        (float(value[i]), (float(cat[i]),)) for i in range(n)
    ]

    # 1. Sensitivity-sampled coreset.
    sens = SensitivityCoreset(eps=args.eps, delta=args.delta, seed=args.seed)
    for v, p in rows:
        sens.add(v, p)
    cs_sens = sens.finalize()
    rep_sens = validate_coverage(
        cs_sens, rows, n_queries=args.queries, level=args.level, seed=args.seed
    )

    # 2. Uniform-baseline coreset of the same size.
    uni = UniformCoreset(m=len(cs_sens), seed=args.seed)
    for v, p in rows:
        uni.add(v, p)
    cs_uni = uni.finalize()
    rep_uni = validate_coverage(
        cs_uni, rows, n_queries=args.queries, level=args.level, seed=args.seed
    )

    def _row(label: str, rep: object) -> str:
        from aqp.eval import ValidationReport

        assert isinstance(rep, ValidationReport)
        return (
            f"{label:<14} m={rep.coreset_size:>6}  "
            f"coverage={rep.coverage:>6.3f}  "
            f"mean_rel_err={rep.mean_relative_error:>7.4f}  "
            f"max_rel_err={rep.max_relative_error:>7.4f}"
        )

    print(f"n_rows={n}  queries={args.queries}  eps={args.eps}  delta={args.delta}")
    print(_row("sensitivity", rep_sens))
    print(_row("uniform", rep_uni))
    return 0


def cmd_quantile(args: argparse.Namespace) -> int:
    import numpy as np

    from aqp.coreset.kll import KLLSketch

    rng = np.random.default_rng(args.seed)
    data = rng.normal(loc=0.0, scale=1.0, size=args.rows)
    sketch = KLLSketch.for_epsilon(args.eps, seed=args.seed)
    for x in data:
        sketch.add(float(x))
    qs = (0.01, 0.1, 0.5, 0.9, 0.99)
    truth = {q: float(np.quantile(data, q)) for q in qs}
    est = {q: sketch.quantile(q) for q in qs}
    print(f"n={args.rows}  eps={args.eps}  k={sketch.k}")
    print(f"{'q':>6} {'truth':>10} {'est':>10} {'err':>10}")
    for q in qs:
        print(f"{q:>6.2f} {truth[q]:>10.4f} {est[q]:>10.4f} {abs(truth[q] - est[q]):>10.4f}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="aqpctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)

    s = sub.add_parser("size", help="report ε,δ → coreset size")
    s.add_argument("--eps", type=float, default=0.05)
    s.add_argument("--delta", type=float, default=0.01)
    s.add_argument("--vc", type=int, default=1)
    s.set_defaults(func=cmd_size)

    v = sub.add_parser("validate", help="empirical coverage / error on synthetic data")
    v.add_argument("--rows", type=int, default=20_000)
    v.add_argument("--queries", type=int, default=200)
    v.add_argument("--eps", type=float, default=0.05)
    v.add_argument("--delta", type=float, default=0.01)
    v.add_argument("--level", type=float, default=0.95)
    v.add_argument("--seed", type=int, default=0)
    v.set_defaults(func=cmd_validate)

    q = sub.add_parser("quantile", help="KLL sketch demo")
    q.add_argument("--rows", type=int, default=50_000)
    q.add_argument("--eps", type=float, default=0.01)
    q.add_argument("--seed", type=int, default=0)
    q.set_defaults(func=cmd_quantile)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
