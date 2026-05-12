"""Cost model for IVM strategy switching.

For each maintenance window we estimate:
    delta_cost  = |Δ| * per_tuple_delta
    full_cost   = |state| * per_tuple_full

If delta_cost > α * full_cost → switch to full recompute.
If delta_cost < β * full_cost → switch back to delta.  (α > β: hysteresis)
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class LinearCostModel:
    per_tuple_delta: float = 1.0
    per_tuple_full: float = 0.5

    def delta_cost(self, delta_size: int) -> float:
        return delta_size * self.per_tuple_delta

    def full_cost(self, state_size: int) -> float:
        return state_size * self.per_tuple_full

    def cost_ratio(self, delta_size: int, state_size: int) -> float:
        full = self.full_cost(state_size)
        if full <= 0:
            return 0.0
        return self.delta_cost(delta_size) / full
