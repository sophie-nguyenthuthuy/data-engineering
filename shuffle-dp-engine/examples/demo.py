"""Demo: ε₀ vs central-ε amplification curve, and a private histogram query."""
from __future__ import annotations

import numpy as np

from src import LocalConfig, private_histogram, shuffle_amplification, MixNode, shuffle


def main() -> None:
    print("=== Shuffle DP Amplification Curves ===\n")
    print(f"{'n':>10} {'ε₀':>6} {'central ε':>12} {'amplification':>15}")
    for n in (100, 1_000, 10_000, 100_000, 1_000_000):
        for eps0 in (1.0, 2.0, 4.0):
            b = shuffle_amplification(eps0=eps0, n=n, delta=1e-6)
            print(f"{n:>10} {eps0:>6} {b.eps_central:>12.4f} {b.amplification:>15.2f}×")

    print("\n=== Private histogram via local RR + shuffler ===\n")
    rng = np.random.default_rng(0)
    cfg = LocalConfig(eps0=3.0, domain_size=5)
    # Simulate a survey: which fruit do you like? (true distribution)
    true_pmf = [0.40, 0.25, 0.20, 0.10, 0.05]
    n = 20_000
    values = list(rng.choice(5, size=n, p=true_pmf))

    # Each user locally randomizes; aggregator shuffles + debiases
    est = private_histogram(values, cfg, rng=rng)
    labels = ["apple", "banana", "cherry", "durian", "elder"]
    print(f"  ε₀ = {cfg.eps0}, n = {n}")
    b = shuffle_amplification(eps0=cfg.eps0, n=n, delta=1e-6)
    print(f"  → central ε via shuffler ≈ {b.eps_central:.4f}\n")
    print(f"  {'label':<10} {'true':>8} {'estimate':>10} {'error':>8}")
    for label, t, e in zip(labels, true_pmf, est):
        print(f"  {label:<10} {t:>8.3f} {e:>10.3f} {abs(t-e):>8.3f}")

    print("\n=== Mix-network round-trip (toy) ===\n")
    nodes = [MixNode.fresh() for _ in range(3)]
    records = [f"vote-{i:03d}".encode() for i in range(10)]
    shuffled = shuffle(records, nodes)
    print("  input  →", records[:5], "...")
    print("  output →", shuffled[:5], "...")
    print("  same multiset:", sorted(records) == sorted(shuffled))
    print("  order changed:", records != shuffled)


if __name__ == "__main__":
    main()
