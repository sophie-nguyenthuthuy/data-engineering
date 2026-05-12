"""Delta IVM vs full-recompute baseline.

For each window-function variant we compare:
  delta: stream updates through IVM
  full:  rebuild ground truth from scratch
"""

from __future__ import annotations

import time

from ivm.nested.max_of_sum import MaxOfSum
from ivm.window.row_number import RowNumberIVM
from ivm.window.sliding_sum import SlidingSumIVM


def bench_row_number(n: int = 5_000) -> dict:
    # Delta path
    rn = RowNumberIVM()
    t0 = time.perf_counter()
    for i in range(n):
        rn.insert("p", float(i), i)
    delta_ms = (time.perf_counter() - t0) * 1000

    # Full-recompute path: sort each insert
    rows: list[tuple[float, int]] = []
    t0 = time.perf_counter()
    for i in range(n):
        rows.append((float(i), i))
        rows.sort()
    full_ms = (time.perf_counter() - t0) * 1000

    return {"op": "row_number", "n": n, "delta_ms": delta_ms,
            "full_ms": full_ms, "speedup": full_ms / delta_ms}


def bench_max_of_sum(n: int = 10_000) -> dict:
    import random
    rng = random.Random(0)
    keys = [f"d{i}" for i in range(50)]

    mos = MaxOfSum()
    t0 = time.perf_counter()
    for _ in range(n):
        k = rng.choice(keys)
        mos.insert(k, rng.uniform(1, 100))
    delta_ms = (time.perf_counter() - t0) * 1000

    # Full path: maintain dict, recompute max each time
    rng2 = random.Random(0)
    sums: dict[str, float] = {}
    t0 = time.perf_counter()
    for _ in range(n):
        k = rng2.choice(keys)
        sums[k] = sums.get(k, 0) + rng2.uniform(1, 100)
        _ = max(sums.values())
    full_ms = (time.perf_counter() - t0) * 1000

    return {"op": "max_of_sum", "n": n, "delta_ms": delta_ms,
            "full_ms": full_ms, "speedup": full_ms / delta_ms}


def bench_sliding_sum(n: int = 5_000, window: int = 50) -> dict:
    sliding = SlidingSumIVM(window_size=window)
    t0 = time.perf_counter()
    for i in range(n):
        sliding.insert("p", float(i), float(i))
    # Query at every position
    for i in range(n):
        sliding.sliding_sum("p", float(i))
    delta_ms = (time.perf_counter() - t0) * 1000

    # Full path: naive slice-and-sum each query
    rows: list[float] = []
    t0 = time.perf_counter()
    for i in range(n):
        rows.append(float(i))
    for i in range(n):
        sum(rows[max(0, i - window + 1): i + 1])
    full_ms = (time.perf_counter() - t0) * 1000
    return {"op": "sliding_sum", "n": n, "delta_ms": delta_ms,
            "full_ms": full_ms, "speedup": full_ms / delta_ms}


def main() -> None:
    print(f"{'op':<12} {'n':>6} {'delta (ms)':>12} {'full (ms)':>12} {'speedup':>8}")
    for fn in (bench_row_number, bench_max_of_sum, bench_sliding_sum):
        r = fn()
        print(f"{r['op']:<12} {r['n']:>6} {r['delta_ms']:>12.1f} "
              f"{r['full_ms']:>12.1f} {r['speedup']:>8.2f}x")


if __name__ == "__main__":
    main()
