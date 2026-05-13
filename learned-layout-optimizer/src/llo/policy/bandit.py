"""Layout-tuning policies.

We expose:

  * ``HeuristicPolicy`` — workload-aware rule (top-K columns + range fractions).
  * ``UCBPolicy`` — UCB1 contextual bandit over a discrete action set.
  * ``EpsilonGreedyPolicy`` — ε-greedy bandit over the same action set.
  * ``ThompsonPolicy`` — Gaussian Thompson sampling bandit.

All bandits share the ``choose() / update(action, reward)`` interface, so a
caller can swap them transparently.
"""

from __future__ import annotations

import math
import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, TypeAlias

if TYPE_CHECKING:
    from llo.workload.profile import WorkloadProfile

ActionKind: TypeAlias = Literal["noop", "sortkey", "zorder", "hilbert"]


@dataclass(frozen=True, slots=True)
class Action:
    """Atomic layout action."""

    kind: ActionKind
    cols: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.kind == "noop" and self.cols:
            raise ValueError("noop takes no columns")
        if self.kind == "sortkey" and len(self.cols) != 1:
            raise ValueError("sortkey takes exactly one column")
        if self.kind == "hilbert" and len(self.cols) < 2:
            raise ValueError("hilbert takes ≥ 2 columns")
        if self.kind == "zorder" and len(self.cols) < 2:
            raise ValueError("zorder takes ≥ 2 columns")

    def __repr__(self) -> str:  # human-friendly key for bandit tables
        if not self.cols:
            return self.kind
        return f"{self.kind}({','.join(self.cols)})"


# ---------------------------------------------------------------- heuristic


def heuristic_action(profile: WorkloadProfile) -> Action:
    """Workload-aware rule:

    * If no observations → ``noop``.
    * If one dominant column → ``sortkey(col)``.
    * If top-2 columns are both heavily range-queried → ``hilbert(c1, c2)``.
    * Otherwise → ``zorder(c1, c2)``.
    """
    top = profile.top_cols(2)
    if not top:
        return Action("noop", ())
    if len(top) == 1:
        return Action("sortkey", (top[0],))
    r0 = profile.range_fraction(top[0])
    r1 = profile.range_fraction(top[1])
    if r0 > 0.5 and r1 > 0.5:
        return Action("hilbert", (top[0], top[1]))
    return Action("zorder", (top[0], top[1]))


@dataclass
class HeuristicPolicy:
    """Wraps :func:`heuristic_action` in the policy interface."""

    profile: WorkloadProfile

    def choose(self) -> Action:
        return heuristic_action(self.profile)

    def update(self, action: Action, reward: float) -> None:
        # heuristic is non-learning
        return None


# ----------------------------------------------------------------- UCB1


@dataclass
class UCBPolicy:
    """UCB1 contextual bandit over a fixed action set."""

    actions: list[Action]
    c: float = math.sqrt(2.0)
    _plays: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _reward_sum: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    _total_plays: int = 0

    def __post_init__(self) -> None:
        if not self.actions:
            raise ValueError("actions must be non-empty")
        if self.c <= 0:
            raise ValueError("c (exploration coefficient) must be > 0")

    def choose(self) -> Action:
        # Every action plays at least once.
        for a in self.actions:
            if self._plays[repr(a)] == 0:
                return a
        best = self.actions[0]
        best_score = -math.inf
        for a in self.actions:
            key = repr(a)
            mean = self._reward_sum[key] / self._plays[key]
            bonus = self.c * math.sqrt(math.log(self._total_plays + 1) / self._plays[key])
            score = mean + bonus
            if score > best_score:
                best_score = score
                best = a
        return best

    def update(self, action: Action, reward: float) -> None:
        key = repr(action)
        self._plays[key] += 1
        self._reward_sum[key] += reward
        self._total_plays += 1

    def mean(self, action: Action) -> float:
        key = repr(action)
        n = self._plays[key]
        return self._reward_sum[key] / n if n else 0.0


# ------------------------------------------------------------- ε-greedy


@dataclass
class EpsilonGreedyPolicy:
    """ε-greedy bandit."""

    actions: list[Action]
    epsilon: float = 0.1
    rng: random.Random = field(default_factory=random.Random)
    _plays: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _reward_sum: dict[str, float] = field(default_factory=lambda: defaultdict(float))

    def __post_init__(self) -> None:
        if not self.actions:
            raise ValueError("actions must be non-empty")
        if not 0.0 <= self.epsilon <= 1.0:
            raise ValueError("epsilon must be in [0, 1]")

    def choose(self) -> Action:
        if self.rng.random() < self.epsilon:
            return self.rng.choice(self.actions)
        best = self.actions[0]
        best_mean = -math.inf
        for a in self.actions:
            key = repr(a)
            mean = self._reward_sum[key] / self._plays[key] if self._plays[key] else 0.0
            if mean > best_mean:
                best_mean = mean
                best = a
        return best

    def update(self, action: Action, reward: float) -> None:
        key = repr(action)
        self._plays[key] += 1
        self._reward_sum[key] += reward


# ------------------------------------------------------- Thompson sampling


@dataclass
class ThompsonPolicy:
    """Gaussian Thompson sampling with known variance.

    Each arm has a Normal posterior over the mean reward. Prior:
    ``μ ~ N(0, prior_var)``; observation noise ``σ²``. We sample one
    posterior mean per arm and act greedily on the samples.
    """

    actions: list[Action]
    prior_var: float = 1.0
    obs_var: float = 1.0
    rng: random.Random = field(default_factory=random.Random)
    _plays: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _reward_sum: dict[str, float] = field(default_factory=lambda: defaultdict(float))

    def __post_init__(self) -> None:
        if not self.actions:
            raise ValueError("actions must be non-empty")
        if self.prior_var <= 0 or self.obs_var <= 0:
            raise ValueError("prior_var and obs_var must be > 0")

    def _posterior(self, key: str) -> tuple[float, float]:
        n = self._plays[key]
        if n == 0:
            return 0.0, self.prior_var
        # Closed form: posterior precision = 1/prior + n/obs_var
        prec = 1.0 / self.prior_var + n / self.obs_var
        var = 1.0 / prec
        mean = var * (self._reward_sum[key] / self.obs_var)
        return mean, var

    def choose(self) -> Action:
        best = self.actions[0]
        best_sample = -math.inf
        for a in self.actions:
            mean, var = self._posterior(repr(a))
            sample = self.rng.gauss(mean, math.sqrt(var))
            if sample > best_sample:
                best_sample = sample
                best = a
        return best

    def update(self, action: Action, reward: float) -> None:
        key = repr(action)
        self._plays[key] += 1
        self._reward_sum[key] += reward


__all__ = [
    "Action",
    "ActionKind",
    "EpsilonGreedyPolicy",
    "HeuristicPolicy",
    "ThompsonPolicy",
    "UCBPolicy",
    "heuristic_action",
]
