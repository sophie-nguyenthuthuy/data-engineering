"""Latency-summary statistics.

Pure-Python percentile helpers — no numpy. Percentiles use the
"nearest-rank" method (PostgreSQL ``percentile_disc`` semantics)
because it's the standard for SLO accounting: ``p99`` returns a value
that *actually appeared* in the sample, not an interpolated estimate.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LatencyStats:
    """Summary of a single query's iteration latencies (seconds)."""

    n: int
    mean: float
    p50: float
    p95: float
    p99: float
    min: float
    max: float

    def to_dict(self) -> dict[str, float | int]:
        return {
            "n": self.n,
            "mean": self.mean,
            "p50": self.p50,
            "p95": self.p95,
            "p99": self.p99,
            "min": self.min,
            "max": self.max,
        }


def summarise(samples: list[float]) -> LatencyStats:
    """Return percentile summary of one query's latency samples."""
    if not samples:
        raise ValueError("summarise requires at least one sample")
    if any(s < 0 for s in samples):
        raise ValueError("latency samples must be ≥ 0")
    sorted_s = sorted(samples)
    n = len(sorted_s)
    return LatencyStats(
        n=n,
        mean=sum(sorted_s) / n,
        p50=_percentile(sorted_s, 0.50),
        p95=_percentile(sorted_s, 0.95),
        p99=_percentile(sorted_s, 0.99),
        min=sorted_s[0],
        max=sorted_s[-1],
    )


def _percentile(sorted_samples: list[float], q: float) -> float:
    if not 0.0 <= q <= 1.0:
        raise ValueError("q must be in [0, 1]")
    n = len(sorted_samples)
    # Nearest-rank method: rank = ceil(q · n), 1-indexed.
    rank = max(1, math.ceil(q * n))
    return sorted_samples[min(rank, n) - 1]


__all__ = ["LatencyStats", "summarise"]
