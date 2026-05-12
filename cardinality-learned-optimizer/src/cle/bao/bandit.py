"""Thompson Sampling bandit for Bao-style plan selection.

Each "arm" is a hint set index (0..14). The bandit maintains a Gaussian
posterior over predicted query cost for each arm, sampled at selection time.

On reward, the posterior is updated using the actual observed log-latency.
This is a simplified conjugate Gaussian-Gaussian model; the full Bao paper
uses a neural network ensemble for uncertainty estimation.  We support both:
  - BayesianBandit: analytic conjugate update (fast, no GPU)
  - NeuralBandit:   uses the QueryOptimizer cost head (accurate, needs data)
"""
from __future__ import annotations
import logging
import math
import random
from typing import Optional
import torch

logger = logging.getLogger(__name__)


class BayesianArm:
    """Gaussian-Gaussian conjugate model for a single arm (hint set)."""

    def __init__(self, prior_mean: float = 5.0, prior_var: float = 4.0) -> None:
        self.mu = prior_mean       # posterior mean (log-ms)
        self.var = prior_var       # posterior variance
        self.n = 0                 # number of observations
        self.obs_var = 1.0         # assumed observation noise variance

    def sample(self) -> float:
        return random.gauss(self.mu, math.sqrt(self.var))

    def update(self, log_latency: float) -> None:
        # Conjugate Gaussian update
        new_var = 1.0 / (1.0 / self.var + 1.0 / self.obs_var)
        self.mu = new_var * (self.mu / self.var + log_latency / self.obs_var)
        self.var = new_var
        self.n += 1


class ThompsonSamplingBandit:
    """Multi-arm bandit over Bao hint sets using Thompson sampling."""

    def __init__(self, num_arms: int = 15) -> None:
        self.arms = [BayesianArm() for _ in range(num_arms)]
        self.history: list[tuple[int, float]] = []   # (arm, log_latency)

    def select(self, exclude: Optional[list[int]] = None) -> int:
        """Sample from each arm's posterior; return arm index with lowest sample."""
        samples = [arm.sample() for arm in self.arms]
        if exclude:
            for i in exclude:
                samples[i] = float("inf")
        return int(min(range(len(samples)), key=lambda i: samples[i]))

    def update(self, arm: int, latency_ms: float) -> None:
        log_lat = math.log(max(latency_ms, 0.001))
        self.arms[arm].update(log_lat)
        self.history.append((arm, log_lat))
        logger.debug("Bandit update arm=%d latency=%.1fms (log=%.3f)", arm, latency_ms, log_lat)

    def best_arm(self) -> int:
        """Return arm with lowest posterior mean (exploitation only)."""
        return int(min(range(len(self.arms)), key=lambda i: self.arms[i].mu))

    def arm_stats(self) -> list[dict]:
        return [
            {"arm": i, "mu": a.mu, "std": math.sqrt(a.var), "n": a.n}
            for i, a in enumerate(self.arms)
        ]


class NeuralBandit:
    """Bao-style bandit that uses QueryOptimizer.cost_head for arm selection.

    Falls back to ThompsonSamplingBandit when model has insufficient data.
    """

    def __init__(self, model, num_arms: int = 15, warmup_per_arm: int = 3) -> None:
        self.model = model
        self.fallback = ThompsonSamplingBandit(num_arms)
        self.num_arms = num_arms
        self.warmup_per_arm = warmup_per_arm
        self._arm_counts = [0] * num_arms

    def _warmed_up(self) -> bool:
        return all(c >= self.warmup_per_arm for c in self._arm_counts)

    def select(self, tree, device=None) -> int:
        if not self._warmed_up():
            # Round-robin during warmup
            arm = min(range(self.num_arms), key=lambda i: self._arm_counts[i])
            logger.debug("Warmup: selecting arm %d", arm)
            return arm

        # Use model cost predictions with Thompson sampling noise
        costs = []
        self.model.eval()
        with torch.no_grad():
            for arm in range(self.num_arms):
                cost = self.model.predict_cost(tree, arm, device=device)
                # Add exploration noise from fallback bandit posterior
                noise = random.gauss(0, math.sqrt(self.fallback.arms[arm].var))
                costs.append(cost + math.exp(noise))

        best = int(min(range(self.num_arms), key=lambda i: costs[i]))
        logger.debug("Neural bandit selected arm=%d (predicted_cost=%.1f)", best, costs[best])
        return best

    def update(self, arm: int, latency_ms: float, tree=None) -> None:
        self._arm_counts[arm] += 1
        self.fallback.update(arm, latency_ms)
