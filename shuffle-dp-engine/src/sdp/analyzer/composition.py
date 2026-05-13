"""Composition accountant.

Sequential composition: k mechanisms each (ε_i, δ_i)-DP → (Σε_i, Σδ_i).
Advanced composition (Dwork-Rothblum-Vadhan): tighter for many mechanisms.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sdp.analyzer.balle import ShuffleBound


@dataclass(frozen=True, slots=True)
class CompositionResult:
    eps_total: float
    delta_total: float
    n_mechanisms: int


def composed_bound(
    bounds: Sequence[ShuffleBound],
    method: str = "basic",
    target_delta: float = 0.0,
) -> CompositionResult:
    """Compose `bounds` sequentially.

    method = "basic"     → naive (Σε, Σδ)
    method = "advanced"  → DRV03 advanced composition w/ additional δ'

    For "advanced" the user passes the additional δ' as `target_delta`.
    """
    if method not in {"basic", "advanced"}:
        raise ValueError(f"unknown method: {method}")
    if method == "advanced" and target_delta <= 0:
        raise ValueError("advanced composition needs target_delta > 0")
    if not bounds:
        return CompositionResult(eps_total=0.0, delta_total=0.0, n_mechanisms=0)
    if method == "basic":
        eps = sum(b.eps_central for b in bounds)
        delta = sum(b.delta for b in bounds)
        return CompositionResult(eps_total=eps, delta_total=delta, n_mechanisms=len(bounds))
    # DRV03 advanced: ε_total ≤ sqrt(2k·ln(1/δ'))·ε_max + k·ε_max·(e^{ε_max} − 1)
    k = len(bounds)
    eps_max = max(b.eps_central for b in bounds)
    delta_max = max(b.delta for b in bounds)
    eps = math.sqrt(2 * k * math.log(1.0 / target_delta)) * eps_max + k * eps_max * (
        math.exp(eps_max) - 1
    )
    delta = k * delta_max + target_delta
    return CompositionResult(eps_total=eps, delta_total=delta, n_mechanisms=k)


__all__ = ["CompositionResult", "composed_bound"]
