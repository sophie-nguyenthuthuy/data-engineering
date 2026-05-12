"""Layout policy: state -> action.

We implement two variants:
  - HeuristicPolicy: human-readable baseline (top-K most-accessed cols)
  - LearnedPolicy: simple bandit-style policy that explores actions and
    converges to the one with highest empirical reward.

Both produce one of these actions:
  ('zorder', [col1, col2, ...])
  ('hilbert', [col1, col2])
  ('sortkey', col)
  ('noop',)
"""
from __future__ import annotations

import math
import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable

from .workload import WorkloadProfile


@dataclass
class Action:
    kind: str
    cols: tuple

    def __repr__(self):
        return f"{self.kind}({','.join(self.cols)})" if self.cols else self.kind


def heuristic_action(profile: WorkloadProfile) -> Action:
    """Pick top-2 cols; use Hilbert if both ranged, else Z-order. Sort-key if
    one column dominates."""
    top = profile.top_cols(2)
    if not top:
        return Action("noop", ())
    if len(top) == 1:
        return Action("sortkey", tuple(top))
    # If both heavily ranged
    r0 = profile.range_fraction(top[0])
    r1 = profile.range_fraction(top[1])
    if r0 > 0.5 and r1 > 0.5:
        return Action("hilbert", tuple(top))
    return Action("zorder", tuple(top))


@dataclass
class UCBPolicy:
    """Contextual bandit (UCB1) over action set. Reward updated externally."""
    actions: list                          # list[Action]
    _plays: dict = field(default_factory=lambda: defaultdict(int))
    _reward_sum: dict = field(default_factory=lambda: defaultdict(float))
    _total_plays: int = 0
    c: float = math.sqrt(2.0)              # exploration coefficient

    def choose(self) -> Action:
        # Play each action at least once
        for a in self.actions:
            if self._plays[repr(a)] == 0:
                return a
        # UCB1
        best = None
        best_score = -math.inf
        for a in self.actions:
            mean = self._reward_sum[repr(a)] / self._plays[repr(a)]
            bonus = self.c * math.sqrt(math.log(self._total_plays + 1) / self._plays[repr(a)])
            score = mean + bonus
            if score > best_score:
                best_score = score
                best = a
        return best

    def update(self, action: Action, reward: float) -> None:
        self._plays[repr(action)] += 1
        self._reward_sum[repr(action)] += reward
        self._total_plays += 1

    def mean(self, action: Action) -> float:
        n = self._plays[repr(action)]
        return self._reward_sum[repr(action)] / n if n else 0.0


__all__ = ["Action", "heuristic_action", "UCBPolicy"]
