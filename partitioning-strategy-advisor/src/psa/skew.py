"""Data-skew detector.

Given a per-value count histogram (``Counter``), we compute two
classical skew measures:

  * **Coefficient of variation** = stdev / mean. CV ≥ 1 is a strong
    skew signal for a candidate partition key.
  * **Top-K share** = fraction of rows in the top-K values. > 50 % in
    the top-3 means a partition by that column would concentrate
    load on a handful of partitions.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SkewReport:
    """Skew diagnostics for one column."""

    name: str
    n: int
    distinct: int
    coefficient_of_variation: float
    top_3_share: float

    def is_skewed(self, *, cv_threshold: float = 1.0, top3_threshold: float = 0.5) -> bool:
        return self.coefficient_of_variation >= cv_threshold or self.top_3_share >= top3_threshold


def detect_skew(name: str, values: list[Any]) -> SkewReport:
    if not name:
        raise ValueError("name must be non-empty")
    n = len(values)
    if n == 0:
        return SkewReport(
            name=name,
            n=0,
            distinct=0,
            coefficient_of_variation=0.0,
            top_3_share=0.0,
        )
    counts = Counter(values)
    distinct = len(counts)
    freqs = list(counts.values())
    mean = sum(freqs) / distinct
    variance = sum((f - mean) ** 2 for f in freqs) / distinct
    cv = math.sqrt(variance) / mean if mean > 0 else 0.0
    top_3 = sum(c for _, c in counts.most_common(3))
    return SkewReport(
        name=name,
        n=n,
        distinct=distinct,
        coefficient_of_variation=cv,
        top_3_share=top_3 / n,
    )


__all__ = ["SkewReport", "detect_skew"]
