"""Per-window strategy switcher: delta vs. full recompute.

For each maintenance window we estimate the cost of:
  - delta propagation: O(|Δ| × per-tuple-delta-work)
  - full recompute: O(|state|)

If delta cost > α × full cost → switch to recompute. Hysteresis prevents
oscillation when costs are close.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque


@dataclass
class StrategyController:
    alpha: float = 0.5          # switch to full when delta_cost > alpha * full_cost
    beta:  float = 0.3          # switch back to delta when delta_cost < beta * full_cost
    _strategy: str = "delta"
    _history: deque = field(default_factory=lambda: deque(maxlen=20))

    def decide(self, delta_size: int, state_size: int,
               per_tuple_delta: float = 1.0,
               per_tuple_full: float = 0.5) -> str:
        delta_cost = delta_size * per_tuple_delta
        full_cost  = state_size * per_tuple_full
        ratio = delta_cost / max(full_cost, 1e-9)
        self._history.append(ratio)
        # Hysteresis
        if self._strategy == "delta" and ratio > self.alpha:
            self._strategy = "full"
        elif self._strategy == "full" and ratio < self.beta:
            self._strategy = "delta"
        return self._strategy

    @property
    def strategy(self) -> str:
        return self._strategy

    @property
    def recent_ratios(self) -> list[float]:
        return list(self._history)


__all__ = ["StrategyController"]
