"""Empirical guarantee validator.

Generates a batch of random predicates from a class with bounded VC
dimension (axis-aligned 1-D ranges), evaluates each on (a) the full
data and (b) a built :class:`Coreset`, then reports:

  * ``coverage`` — fraction of queries whose true answer lies inside
    the coreset's confidence interval.
  * ``mean_relative_error`` — mean ``|est − true| / max(|true|, 1)``.
  * ``max_relative_error``.

This is the standard end-to-end check an AQP engine ships: "do the
intervals we claim actually contain the truth (1 − δ) of the time?"
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from aqp.queries.predicates import range_pred

if TYPE_CHECKING:
    from aqp.coreset.core import Coreset
    from aqp.queries.predicates import Payload


@dataclass(frozen=True, slots=True)
class ValidationReport:
    """Summary statistics from :func:`validate_coverage`."""

    n_queries: int
    coverage: float
    mean_relative_error: float
    max_relative_error: float
    coreset_size: int


def validate_coverage(
    coreset: Coreset,
    rows: list[tuple[float, Payload]],
    n_queries: int = 200,
    col: int = 0,
    level: float = 0.95,
    seed: int | None = 0,
) -> ValidationReport:
    """Evaluate ``n_queries`` random 1-D range queries on ``coreset`` vs ``rows``."""
    if n_queries < 1:
        raise ValueError("n_queries must be ≥ 1")
    if not rows:
        return ValidationReport(0, 1.0, 0.0, 0.0, len(coreset))

    rng = np.random.default_rng(seed)
    col_vals = np.array([float(p[col]) for _, p in rows])
    lo_min, hi_max = float(col_vals.min()), float(col_vals.max())

    covered = 0
    rel_errors: list[float] = []
    for _ in range(n_queries):
        lo = float(rng.uniform(lo_min, hi_max))
        hi = float(rng.uniform(lo, hi_max))
        pred = range_pred(col, lo, hi)
        truth = sum(v for v, p in rows if pred(p))
        ci = coreset.sum_confidence_interval(pred, level=level)
        if ci.contains(truth):
            covered += 1
        denom = max(abs(truth), 1.0)
        rel_errors.append(abs(ci.estimate - truth) / denom)

    return ValidationReport(
        n_queries=n_queries,
        coverage=covered / n_queries,
        mean_relative_error=sum(rel_errors) / n_queries,
        max_relative_error=max(rel_errors),
        coreset_size=len(coreset),
    )


__all__ = ["ValidationReport", "validate_coverage"]
