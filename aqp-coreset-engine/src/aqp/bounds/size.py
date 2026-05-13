"""Coreset-size formulas.

We expose two off-the-shelf bounds:

  * :func:`coreset_size_sum` — Feldman-Langberg sensitivity-sampling size
    for SUM/COUNT queries with VC-dimension ``vc``: ``m = ⌈c · (1/ε²) ·
    (vc + log(1/δ))⌉``. We take ``c = 1`` (folklore constant for sums
    over predicates with bounded sensitivity); the underlying coreset
    implementation accepts a tighter ``m`` if the user wants it.

  * :func:`hoeffding_count_size` — pure Hoeffding bound on the sample
    size required for ``ε``-relative error of a COUNT in [0, 1] with
    probability ≥ 1 − δ.
"""

from __future__ import annotations

import math


def _validate(eps: float, delta: float) -> None:
    if not 0.0 < eps < 1.0:
        raise ValueError("eps must be in (0, 1)")
    if not 0.0 < delta < 1.0:
        raise ValueError("delta must be in (0, 1)")


def coreset_size_sum(eps: float, delta: float, vc: int = 1) -> int:
    """Recommended coreset size for SUM under a class of VC-dim ``vc``."""
    _validate(eps, delta)
    if vc < 1:
        raise ValueError("vc must be ≥ 1")
    m = (1.0 / (eps * eps)) * (vc + math.log(1.0 / delta))
    return max(1, math.ceil(m))


def hoeffding_count_size(eps: float, delta: float) -> int:
    """Hoeffding bound on sample size for ε-relative COUNT error."""
    _validate(eps, delta)
    m = math.log(2.0 / delta) / (2.0 * eps * eps)
    return max(1, math.ceil(m))


__all__ = ["coreset_size_sum", "hoeffding_count_size"]
