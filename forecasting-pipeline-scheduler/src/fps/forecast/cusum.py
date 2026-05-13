"""CUSUM drift detector for runtime forecasts.

Page (1954). Maintains two cumulative sums of the centred + scaled
residuals — one for upward drift, one for downward. When either
exceeds a threshold the detector fires.

We use the classical formulation in terms of the standardised
deviation ``z = (xᵢ − μ) / σ`` with reference value ``k`` (typically
0.5) and decision threshold ``h`` (typically 4–5 σ):

    S⁺ᵢ = max(0, S⁺ᵢ₋₁ + zᵢ − k)
    S⁻ᵢ = min(0, S⁻ᵢ₋₁ + zᵢ + k)

This is the per-task drift signal the scheduler uses to invalidate a
forecast and force the :class:`LognormalForecaster` to re-fit.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CUSUMDetector:
    """Two-sided CUSUM over standardised residuals."""

    mean: float
    sigma: float
    k: float = 0.5
    h: float = 5.0
    s_pos: float = 0.0
    s_neg: float = 0.0
    n: int = 0
    fired: bool = False

    def __post_init__(self) -> None:
        if self.sigma <= 0:
            raise ValueError("sigma must be > 0")
        if self.k < 0:
            raise ValueError("k must be ≥ 0")
        if self.h <= 0:
            raise ValueError("h must be > 0")

    def update(self, value: float) -> bool:
        """Feed one observation; return ``True`` when drift fires."""
        z = (value - self.mean) / self.sigma
        self.s_pos = max(0.0, self.s_pos + z - self.k)
        self.s_neg = min(0.0, self.s_neg + z + self.k)
        self.n += 1
        if self.s_pos >= self.h or self.s_neg <= -self.h:
            self.fired = True
        return self.fired

    def reset(self) -> None:
        self.s_pos = 0.0
        self.s_neg = 0.0
        self.n = 0
        self.fired = False


__all__ = ["CUSUMDetector"]
