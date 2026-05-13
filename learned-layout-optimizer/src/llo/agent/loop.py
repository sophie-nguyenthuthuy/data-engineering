"""Closed-loop layout agent.

The agent ties the four subsystems together:

  1. ingest queries → update :class:`WorkloadProfile`.
  2. ask a :class:`Policy` for the next action.
  3. apply the action and replay the recent workload to measure reward.
  4. feed the reward back into the policy.
  5. consult a :class:`DriftDetector`; if drift is detected, recalibrate
     and reset bandit statistics so the agent can re-explore.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from llo.replay.pages import reward as replay_reward
from llo.workload.drift import DriftDetector

if TYPE_CHECKING:
    import numpy as np
    from numpy.typing import NDArray

    from llo.policy.bandit import Action
    from llo.workload.profile import Query, WorkloadProfile


class _Policy(Protocol):
    """Minimal interface every bandit/heuristic implements."""

    def choose(self) -> Action: ...
    def update(self, action: Action, reward: float) -> None: ...


@dataclass
class StepLog:
    step: int
    action: Action
    reward: float
    drift: float
    drift_event: bool


@dataclass
class LayoutAgent:
    """Closed-loop agent acting on a single in-memory table."""

    data: NDArray[np.integer]
    columns: list[str]
    policy: _Policy
    profile: WorkloadProfile
    drift: DriftDetector = field(default_factory=DriftDetector)
    io_cost: float = 100.0
    window: int = 200
    _recent: list[Query] = field(default_factory=list)
    _step: int = 0
    history: list[StepLog] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.data.ndim != 2:
            raise ValueError("data must be 2-D")
        if self.data.shape[1] != len(self.columns):
            raise ValueError("columns count does not match data width")
        if self.window <= 0:
            raise ValueError("window must be > 0")
        # Snapshot an initial baseline so the first drift score is meaningful.
        self.drift.calibrate(self.profile)

    # ------------------------------------------------------------------ API

    def observe(self, q: Query) -> None:
        """Record a query against the profile (without acting)."""
        self.profile.observe(q)
        self._recent.append(q)
        if len(self._recent) > self.window:
            self._recent = self._recent[-self.window :]

    def step(self) -> StepLog:
        """Pick an action, evaluate it, update the policy, return the log."""
        action = self.policy.choose()
        r = replay_reward(self.data, self.columns, action, self._recent, io_cost=self.io_cost)
        self.policy.update(action, r)
        drift = self.drift.score(self.profile)
        event = self.drift.has_drifted(self.profile)
        if event:
            # Re-baseline so the next step gets a clean comparison.
            self.drift.calibrate(self.profile)
        log = StepLog(step=self._step, action=action, reward=r, drift=drift, drift_event=event)
        self.history.append(log)
        self._step += 1
        return log

    def run(self, queries: list[Query], act_every: int = 50) -> list[StepLog]:
        """Convenience: stream ``queries`` and act every ``act_every`` items."""
        if act_every <= 0:
            raise ValueError("act_every must be > 0")
        for i, q in enumerate(queries, start=1):
            self.observe(q)
            if i % act_every == 0:
                self.step()
        return self.history


__all__ = ["LayoutAgent", "StepLog"]
