"""Demo: build a coreset, run multiple queries with confidence intervals."""
from __future__ import annotations

import random

from src import SumCoreset, QuantileSketch


def main() -> None:
    rng = random.Random(123)
    rows = []
    for _ in range(50_000):
        category = rng.choice(["A", "B", "C", "D"])
        # B and C have higher amounts to make the predicate interesting
        scale = {"A": 30, "B": 80, "C": 120, "D": 50}[category]
        amount = rng.gauss(scale, 15)
        rows.append((amount, (category,)))

    print(f"Full dataset: {len(rows)} rows")
    print(f"True total: {sum(v for v, _ in rows):.2f}")
    print(f"True total for B: {sum(v for v, p in rows if p[0] == 'B'):.2f}")
    print(f"True total for C: {sum(v for v, p in rows if p[0] == 'C'):.2f}")

    # Build coreset
    cs = SumCoreset(eps=0.05, delta=0.01, seed=42)
    for v, p in rows:
        cs.add(v, p)
    coreset = cs.finalize()
    print(f"\nCoreset size: {len(coreset)} rows ({100*len(coreset)/len(rows):.2f}% of full)")

    # Query the coreset
    print("\nQuery results (estimate / 99% CI):")
    for cat in ["A", "B", "C", "D"]:
        est, lo, hi = coreset.confidence_interval(predicate=lambda p, c=cat: p[0] == c)
        true_val = sum(v for v, p in rows if p[0] == cat)
        in_ci = "✓" if lo <= true_val <= hi else "✗"
        print(f"  Total[{cat}]: {est:>10.2f}   CI=[{lo:>10.2f}, {hi:>10.2f}]"
              f"   true={true_val:>10.2f}   {in_ci}")

    # Quantile sketch on amounts
    print("\nQuantile sketch (all amounts):")
    qs = QuantileSketch(eps=0.01)
    for v, _ in rows:
        qs.add(v)
    for q in [0.5, 0.9, 0.99]:
        sorted_vals = sorted(v for v, _ in rows)
        true_q = sorted_vals[int(q * len(sorted_vals))]
        est = qs.quantile(q)
        print(f"  q={q}  estimate={est:.2f}  true={true_q:.2f}  err={abs(est-true_q):.2f}")


if __name__ == "__main__":
    main()
