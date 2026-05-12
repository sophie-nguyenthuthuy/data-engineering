"""Write throughput at various epsilon values, plus write-amplification."""

from __future__ import annotations

import time

from sortedcontainers import SortedDict

from beps.stats.amplification import WriteAmpStats
from beps.tree.tree import BEpsilonTree


def bench(epsilon: float, n: int = 10_000) -> dict:
    stats = WriteAmpStats()
    t = BEpsilonTree(node_size=16, epsilon=epsilon, amp_stats=stats)
    start = time.perf_counter()
    for i in range(n):
        t.put(f"k{i:08d}".encode(), i)
    elapsed = time.perf_counter() - start
    snap = stats.snapshot()
    return {
        "epsilon": epsilon, "n": n,
        "ms": elapsed * 1000, "qps": n / elapsed,
        "depth": t.depth(), "nodes": t.node_count(),
        "write_amp": snap["write_amplification"],
        "splits": snap["splits"],
    }


def baseline_dict(n: int = 10_000) -> dict:
    start = time.perf_counter()
    d: dict[bytes, int] = {}
    for i in range(n):
        d[f"k{i:08d}".encode()] = i
    return {"label": "dict", "n": n,
            "ms": (time.perf_counter() - start) * 1000}


def baseline_sorted(n: int = 10_000) -> dict:
    start = time.perf_counter()
    sd = SortedDict()
    for i in range(n):
        sd[f"k{i:08d}".encode()] = i
    return {"label": "SortedDict", "n": n,
            "ms": (time.perf_counter() - start) * 1000}


def main() -> None:
    n = 10_000
    print("=== Write benchmark ===")
    print(f"{'epsilon':>8} {'n':>6} {'ms':>10} {'qps':>10} {'depth':>5} "
          f"{'nodes':>5} {'write_amp':>10} {'splits':>6}")
    for eps in (0.1, 0.3, 0.5, 0.7, 0.9):
        r = bench(eps, n)
        print(f"{r['epsilon']:>8} {r['n']:>6} {r['ms']:>10.1f} {r['qps']:>10,.0f} "
              f"{r['depth']:>5} {r['nodes']:>5} {r['write_amp']:>10.2f} {r['splits']:>6}")

    print("\n=== Baselines ===")
    for fn in (baseline_dict, baseline_sorted):
        r = fn(n)
        print(f"{r['label']:<12} ms={r['ms']:.1f}")


if __name__ == "__main__":
    main()
