"""Operation generators for benchmarks."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


def mixed_workload(
    n_ops: int,
    n_keys: int,
    insert_fraction: float = 0.8,
    seed: int = 0,
) -> Iterator[tuple[str, object, float, float]]:
    """Yield (op, partition, t, value)."""
    rng = random.Random(seed)
    for i in range(n_ops):
        op = "insert" if rng.random() < insert_fraction else "delete"
        partition = f"p{rng.randint(0, n_keys - 1)}"
        t = float(i)
        value = rng.uniform(0, 1000)
        yield op, partition, t, value


def burst_workload(
    n_ops: int,
    n_keys: int,
    burst_size: int = 100,
    seed: int = 0,
) -> Iterator[tuple[str, object, float, float]]:
    """Bursts of inserts followed by a few deletes — simulates real-world
    arrival patterns where the strategy switcher may want to flip."""
    rng = random.Random(seed)
    i = 0
    while i < n_ops:
        # Burst of inserts
        for _ in range(min(burst_size, n_ops - i)):
            partition = f"p{rng.randint(0, n_keys - 1)}"
            yield "insert", partition, float(i), rng.uniform(0, 1000)
            i += 1
        # Small batch of deletes
        for _ in range(min(burst_size // 10, n_ops - i)):
            partition = f"p{rng.randint(0, n_keys - 1)}"
            yield "delete", partition, float(i - burst_size), rng.uniform(0, 1000)
            i += 1


def sliding_workload(
    n_ops: int,
    n_partitions: int,
    window_size: int = 100,
    seed: int = 0,
) -> Iterator[tuple[str, object, float, float]]:
    """Sliding-window pattern: insert n, then delete the oldest per partition."""
    rng = random.Random(seed)
    counters = {f"p{i}": 0 for i in range(n_partitions)}
    for _ in range(n_ops):
        p = f"p{rng.randint(0, n_partitions - 1)}"
        t = float(counters[p])
        v = rng.uniform(0, 1000)
        yield "insert", p, t, v
        counters[p] += 1
        if counters[p] > window_size:
            old_t = float(counters[p] - window_size - 1)
            yield "delete", p, old_t, v
