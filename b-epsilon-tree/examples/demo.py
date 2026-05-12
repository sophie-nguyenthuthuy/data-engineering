"""Demo: insert 10k random keys, mix in deletes, measure perf at different ε."""
from __future__ import annotations

import random
import time

from src import BEpsilonTree, EpsilonTuner


def benchmark(eps: float, n: int = 10_000, seed: int = 0):
    rng = random.Random(seed)
    t = BEpsilonTree(epsilon=eps)
    # Write-heavy
    t0 = time.perf_counter()
    for i in range(n):
        t.put(rng.randint(0, n // 2), i)
    write_time = time.perf_counter() - t0
    # Read pass
    t0 = time.perf_counter()
    hits = 0
    for _ in range(n):
        if t.get(rng.randint(0, n // 2)) is not None:
            hits += 1
    read_time = time.perf_counter() - t0
    return {
        "epsilon": eps,
        "depth": t.depth(),
        "size": t.size(),
        "write_ms": 1000 * write_time,
        "read_ms": 1000 * read_time,
        "hits": hits,
    }


def main():
    print(f"{'ε':>4} {'depth':>6} {'size':>6} {'writes (ms)':>12} {'reads (ms)':>11}")
    for eps in (0.1, 0.3, 0.5, 0.7, 0.9):
        r = benchmark(eps)
        print(f"{r['epsilon']:>4} {r['depth']:>6} {r['size']:>6} "
              f"{r['write_ms']:>12.1f} {r['read_ms']:>11.1f}")

    print("\nEpsilon tuner adapts to workload:")
    tuner = EpsilonTuner()
    for _ in range(800): tuner.observe("write")
    for _ in range(200): tuner.observe("read")
    print(f"  80% writes → recommended ε = {tuner.recommend():.3f}")
    tuner._events.clear()
    for _ in range(800): tuner.observe("read")
    for _ in range(200): tuner.observe("write")
    print(f"  80% reads  → recommended ε = {tuner.recommend():.3f}")


if __name__ == "__main__":
    main()
