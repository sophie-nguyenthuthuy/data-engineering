"""
Workload generators for benchmarking learned vs. classic index structures.

All generators produce sorted unique integer keys suitable for index training
and a separate query set drawn from the same distribution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np


@dataclass
class Workload:
    name: str
    keys: np.ndarray       # sorted unique keys used to build the index
    queries: np.ndarray    # query keys (may include keys not in the set)
    hit_rate: float        # fraction of queries that are expected hits


def uniform(
    n: int = 1_000_000,
    q: int = 100_000,
    key_range: int = 10_000_000,
    rng: np.random.Generator | None = None,
) -> Workload:
    """Uniformly distributed keys in [1, key_range]."""
    rng = rng or np.random.default_rng(42)
    keys = np.sort(rng.choice(key_range, size=n, replace=False)).astype(np.int64)
    queries = rng.choice(key_range, size=q).astype(np.int64)
    return Workload("uniform", keys, queries, hit_rate=n / key_range)


def zipfian(
    n: int = 1_000_000,
    q: int = 100_000,
    alpha: float = 1.2,
    key_range: int = 10_000_000,
    rng: np.random.Generator | None = None,
) -> Workload:
    """
    Zipfian-skewed keys: a small fraction of the key space is queried
    disproportionately often.  *alpha* controls skew (higher = more skewed).
    """
    rng = rng or np.random.default_rng(42)
    # Generate base keys uniformly, but bias queries via Zipf ranks
    keys = np.sort(rng.choice(key_range, size=n, replace=False)).astype(np.int64)
    ranks = rng.zipf(alpha, size=q)
    ranks = np.clip(ranks, 1, n) - 1  # map to valid index
    queries = keys[ranks]
    return Workload(f"zipfian_α{alpha}", keys, queries, hit_rate=1.0)


def time_series(
    n: int = 1_000_000,
    q: int = 100_000,
    start_ts: int = 1_600_000_000,  # Unix timestamp epoch
    interval_ms: int = 100,
    jitter_pct: float = 0.05,
    rng: np.random.Generator | None = None,
) -> Workload:
    """
    Monotonically increasing timestamps with small Gaussian jitter — mimics
    sensor / event-log ingestion at roughly ``1000/interval_ms`` events/sec.
    """
    rng = rng or np.random.default_rng(42)
    base = np.arange(n, dtype=np.int64) * interval_ms + start_ts * 1000
    jitter = (rng.normal(0, interval_ms * jitter_pct, size=n)).astype(np.int64)
    keys = np.sort(np.unique(base + jitter)).astype(np.int64)[:n]
    # Queries: 80% recency bias (last 10% of range), 20% random
    n_recent = int(q * 0.8)
    n_rand = q - n_recent
    recent_start = max(0, len(keys) - len(keys) // 10)
    recent_queries = rng.choice(keys[recent_start:], size=n_recent)
    rand_queries = rng.choice(keys, size=n_rand)
    queries = np.concatenate([recent_queries, rand_queries])
    rng.shuffle(queries)
    return Workload("time_series", keys, queries.astype(np.int64), hit_rate=1.0)


def mixed_drift(
    n: int = 1_000_000,
    q_per_phase: int = 50_000,
    rng: np.random.Generator | None = None,
) -> tuple[Workload, Workload]:
    """
    Return two workloads representing BEFORE and AFTER a distribution shift.

    Phase 1: uniform distribution (index trained here)
    Phase 2: Zipfian distribution over the same key space (drift)
    """
    rng = rng or np.random.default_rng(42)
    w_before = uniform(n, q_per_phase, rng=rng)
    w_after = zipfian(n, q_per_phase, alpha=1.5, rng=rng)
    # Use same key set so index is valid but query distribution shifts
    w_after = Workload(
        "post_drift_zipfian",
        w_before.keys,   # same keys
        w_after.queries,  # Zipfian queries
        hit_rate=w_after.hit_rate,
    )
    return w_before, w_after


ALL_WORKLOADS: list[Callable[[], Workload]] = [
    lambda: uniform(),
    lambda: zipfian(alpha=1.2),
    lambda: zipfian(alpha=1.5),
    lambda: time_series(),
]
