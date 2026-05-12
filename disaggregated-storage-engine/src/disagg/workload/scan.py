"""OLAP-shaped workloads."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from disagg.core.page import PageId

if TYPE_CHECKING:
    from collections.abc import Iterator


def scan_workload(n_pages: int, tenant: int = 0, n_passes: int = 1) -> Iterator[PageId]:
    """Sequential scan: every page in order."""
    for _ in range(n_passes):
        for p in range(n_pages):
            yield PageId(tenant=tenant, page_no=p)


def zipf_workload(
    n_pages: int,
    n_ops: int,
    alpha: float = 1.2,
    tenant: int = 0,
    seed: int = 0,
) -> Iterator[PageId]:
    """Zipfian distribution — heavy-tailed page access, classic OLTP shape."""
    rng = random.Random(seed)
    # Precompute Zipf probabilities: P(rank i) ∝ 1 / i^alpha
    # For efficiency, build a CDF.
    weights = [1.0 / ((i + 1) ** alpha) for i in range(n_pages)]
    total = sum(weights)
    cdf: list[float] = []
    cum = 0.0
    for w in weights:
        cum += w / total
        cdf.append(cum)
    from bisect import bisect_left

    for _ in range(n_ops):
        r = rng.random()
        rank = bisect_left(cdf, r)
        yield PageId(tenant=tenant, page_no=rank)
