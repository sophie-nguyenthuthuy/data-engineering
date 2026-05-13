"""CLI."""

from __future__ import annotations

import argparse


def cmd_amplification(args: argparse.Namespace) -> int:
    from sdp.analyzer.balle import shuffle_amplification

    if args.n is not None and args.eps0 is not None:
        b = shuffle_amplification(eps0=args.eps0, n=args.n, delta=args.delta)
        print(f"ε₀     = {args.eps0}")
        print(f"n      = {args.n}")
        print(f"δ      = {args.delta}")
        print(f"ε_cdp  = {b.eps_central:.6f}")
        print(f"amp    = {b.amplification:.2f}x")
        return 0

    print(f"{'n':>10} {'eps0':>6} {'central eps':>12} {'amp':>8}")
    for n in (100, 1_000, 10_000, 100_000, 1_000_000):
        for e in (1.0, 2.0, 4.0):
            b = shuffle_amplification(eps0=e, n=n, delta=args.delta)
            print(f"{n:>10} {e:>6} {b.eps_central:>12.4f} {b.amplification:>8.2f}x")
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    import numpy as np

    from sdp.local.randomizers import LocalConfig
    from sdp.queries.histogram import private_histogram

    rng = np.random.default_rng(0)
    cfg = LocalConfig(eps0=args.eps0, domain_size=4)
    true_pmf = [0.5, 0.2, 0.2, 0.1]
    samples = list(rng.choice(4, size=args.n, p=true_pmf))
    est = private_histogram(samples, cfg, rng=rng)
    print(f"ε₀={cfg.eps0}  n={args.n}")
    print(f"{'label':<8} {'true':>8} {'est':>8} {'err':>8}")
    for i, (t, e) in enumerate(zip(true_pmf, est, strict=False)):
        print(f"v{i:<7} {t:>8.3f} {e:>8.3f} {abs(t - e):>8.3f}")
    return 0


def cmd_info(_args: argparse.Namespace) -> int:
    from sdp import __version__

    print(f"shuffle-dp-engine {__version__}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="shufflectl")
    sub = p.add_subparsers(dest="cmd", required=True)
    amp = sub.add_parser("amplification")
    amp.add_argument("--eps0", type=float, default=None)
    amp.add_argument("--n", type=int, default=None)
    amp.add_argument("--delta", type=float, default=1e-6)
    amp.set_defaults(func=cmd_amplification)
    demo = sub.add_parser("demo")
    demo.add_argument("--n", type=int, default=20_000)
    demo.add_argument("--eps0", type=float, default=3.0)
    demo.set_defaults(func=cmd_demo)
    sub.add_parser("info").set_defaults(func=cmd_info)
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
