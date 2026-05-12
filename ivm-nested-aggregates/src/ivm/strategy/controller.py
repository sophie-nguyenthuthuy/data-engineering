"""Strategy controller with hysteresis.

  alpha:  switch to full when ratio > alpha
  beta:   switch to delta when ratio < beta
  alpha > beta to avoid oscillation
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field

from ivm.strategy.cost_model import LinearCostModel


@dataclass
class StrategyController:
    alpha: float = 0.5
    beta: float = 0.3
    history_size: int = 20
    cost_model: LinearCostModel = field(default_factory=LinearCostModel)
    _strategy: str = "delta"
    _history: deque[float] = field(default_factory=lambda: deque(maxlen=20))
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.history_size != self._history.maxlen:
            self._history = deque(self._history, maxlen=self.history_size)

    def decide(self, delta_size: int, state_size: int) -> str:
        with self._lock:
            ratio = self.cost_model.cost_ratio(delta_size, state_size)
            self._history.append(ratio)
            if self._strategy == "delta" and ratio > self.alpha:
                self._strategy = "full"
            elif self._strategy == "full" and ratio < self.beta:
                self._strategy = "delta"
            return self._strategy

    @property
    def strategy(self) -> str:
        with self._lock:
            return self._strategy

    @property
    def recent_ratios(self) -> list[float]:
        with self._lock:
            return list(self._history)


__all__ = ["StrategyController"]
