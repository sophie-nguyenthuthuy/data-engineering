"""Read latency: how does epsilon affect lookup time?"""

from __future__ import annotations

import random
import time

from beps.tree.tree import BEpsilonTree


def bench(epsilon: float, n_keys: int = 10_000, n_reads: int = 5_000) -> dict:
    t = BEpsilonTree(node_size=16, epsilon=epsilon)
    for i in range(n_keys):
        t.put(f"k{i:08d}".encode(), i)
    rng = random.Random(0)
    sample = [f"k{rng.randint(0, n_keys - 1):08d}".encode() for _ in range(n_reads)]

    start = time.perf_counter()
    for k in sample:
        t.get(k)
    elapsed = time.perf_counter() - start

    return {"epsilon": epsilon, "reads": n_reads, "ms": elapsed * 1000,
            "qps": n_reads / elapsed, "depth": t.depth(),
            "buffer_remaining": t.buffer_total()}


def main() -> None:
    print(f"{'epsilon':>8} {'reads':>6} {'ms':>10} {'qps':>10} {'depth':>5} {'buf':>6}")
    for eps in (0.1, 0.3, 0.5, 0.7, 0.9):
        r = bench(eps)
        print(f"{r['epsilon']:>8} {r['reads']:>6} {r['ms']:>10.1f} "
              f"{r['qps']:>10,.0f} {r['depth']:>5} {r['buffer_remaining']:>6}")


if __name__ == "__main__":
    main()
