"""Lookup benchmark: ART vs dict vs SortedDict."""

from __future__ import annotations

import random
import time
from typing import TYPE_CHECKING

from sortedcontainers import SortedDict

if TYPE_CHECKING:
    from collections.abc import Callable

from art_mvcc.art.tree import ART


def gen_keys(n: int, seed: int = 0) -> list[bytes]:
    rng = random.Random(seed)
    return [bytes(rng.randint(0, 255) for _ in range(rng.randint(4, 16)))
            for _ in range(n)]


def time_op(fn: Callable[[], None], reps: int = 1) -> float:
    t0 = time.perf_counter()
    for _ in range(reps):
        fn()
    return (time.perf_counter() - t0) / reps


def bench_insert(n: int) -> dict:
    keys = gen_keys(n)
    values = list(range(n))

    art = ART()
    t_art = time_op(lambda: [art.put(k, v) for k, v in zip(keys, values, strict=False)])

    d = {}
    t_dict = time_op(lambda: [d.__setitem__(k, v) for k, v in zip(keys, values, strict=False)])

    sd = SortedDict()
    t_sorted = time_op(lambda: [sd.__setitem__(k, v) for k, v in zip(keys, values, strict=False)])

    return {"n": n, "art_ms": t_art * 1000, "dict_ms": t_dict * 1000,
            "sortdict_ms": t_sorted * 1000}


def bench_lookup(n: int) -> dict:
    keys = gen_keys(n)

    art = ART()
    for i, k in enumerate(keys):
        art.put(k, i)
    d = dict(zip(keys, range(n), strict=False))
    sd = SortedDict(zip(keys, range(n), strict=False))

    sample = random.Random(42).sample(keys, min(n, 10_000))
    t_art = time_op(lambda: [art.get(k) for k in sample])
    t_dict = time_op(lambda: [d.get(k) for k in sample])
    t_sorted = time_op(lambda: [sd.get(k) for k in sample])

    return {"n": n, "lookups": len(sample),
            "art_ms": t_art * 1000, "dict_ms": t_dict * 1000,
            "sortdict_ms": t_sorted * 1000}


def bench_range(n: int) -> dict:
    keys = gen_keys(n)
    keys_sorted = sorted(set(keys))[:n // 10]

    art = ART()
    sd = SortedDict()
    for i, k in enumerate(keys):
        art.put(k, i)
        sd[k] = i

    lo, hi = keys_sorted[0], keys_sorted[-1]

    t_art = time_op(lambda: list(art.iter_range(lo, hi)))
    t_sorted = time_op(lambda: list(sd.irange(lo, hi, inclusive=(True, False))))

    return {"n": n, "art_ms": t_art * 1000, "sortdict_ms": t_sorted * 1000}


def main() -> None:
    print("=== Insert ===")
    print(f"{'n':>8} {'ART (ms)':>10} {'dict (ms)':>10} {'SortDict (ms)':>14}")
    for n in (1_000, 10_000, 100_000):
        r = bench_insert(n)
        print(f"{r['n']:>8} {r['art_ms']:>10.1f} {r['dict_ms']:>10.1f} {r['sortdict_ms']:>14.1f}")

    print("\n=== Lookup (10k random reads) ===")
    print(f"{'n':>8} {'ART (ms)':>10} {'dict (ms)':>10} {'SortDict (ms)':>14}")
    for n in (1_000, 10_000, 100_000):
        r = bench_lookup(n)
        print(f"{r['n']:>8} {r['art_ms']:>10.1f} {r['dict_ms']:>10.1f} {r['sortdict_ms']:>14.1f}")

    print("\n=== Range scan ===")
    print(f"{'n':>8} {'ART (ms)':>10} {'SortDict (ms)':>14}")
    for n in (10_000, 100_000):
        r = bench_range(n)
        print(f"{r['n']:>8} {r['art_ms']:>10.1f} {r['sortdict_ms']:>14.1f}")


if __name__ == "__main__":
    main()
