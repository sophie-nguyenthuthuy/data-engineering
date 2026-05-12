"""
Bayesian quality dimension scorers.

Each scorer maintains a Beta(alpha, beta) posterior representing
P(true quality rate).  After observing (successes, total) from a batch
the posterior updates analytically:

    alpha_new = alpha_old + successes
    beta_new  = beta_old  + (total - successes)

P(healthy | data) = P(rate > health_threshold) = 1 - Beta_CDF(threshold; alpha, beta)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
from scipy.stats import beta as beta_dist

from .models import BatchObservation, DQDimension, PosteriorState


class _BaseDimensionScorer:
    """Beta-Binomial Bayesian scorer for a single quality dimension."""

    dimension: DQDimension

    def __init__(
        self,
        prior_alpha: float = 2.0,
        prior_beta: float = 2.0,
        health_threshold: float = 0.95,
    ):
        if prior_alpha <= 0 or prior_beta <= 0:
            raise ValueError("Prior hyperparameters must be positive.")
        if not 0 < health_threshold < 1:
            raise ValueError("health_threshold must be in (0, 1).")

        self.prior_alpha = prior_alpha
        self.prior_beta = prior_beta
        self.health_threshold = health_threshold
        self._state = PosteriorState(
            dimension=self.dimension,
            alpha=prior_alpha,
            beta=prior_beta,
        )

    @property
    def state(self) -> PosteriorState:
        return self._state

    def observe(self, obs: BatchObservation) -> PosteriorState:
        """Update posterior with one batch observation and return new state."""
        failures = obs.total - obs.successes
        self._state = PosteriorState(
            dimension=self.dimension,
            alpha=self._state.alpha + obs.successes,
            beta=self._state.beta + failures,
            batch_count=self._state.batch_count + 1,
            last_updated=obs.timestamp,
        )
        return self._state

    def p_healthy(self, state: Optional[PosteriorState] = None) -> float:
        """P(quality_rate > health_threshold) under the current posterior."""
        s = state or self._state
        return float(1.0 - beta_dist.cdf(self.health_threshold, s.alpha, s.beta))

    def credible_interval(
        self,
        mass: float = 0.95,
        state: Optional[PosteriorState] = None,
    ) -> tuple[float, float]:
        """Equal-tailed credible interval for the quality rate."""
        s = state or self._state
        lo = (1 - mass) / 2
        hi = 1 - lo
        return (
            float(beta_dist.ppf(lo, s.alpha, s.beta)),
            float(beta_dist.ppf(hi, s.alpha, s.beta)),
        )

    def pdf_curve(
        self,
        n_points: int = 300,
        state: Optional[PosteriorState] = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Return (x, pdf) arrays for plotting the posterior density."""
        s = state or self._state
        x = np.linspace(0, 1, n_points)
        y = beta_dist.pdf(x, s.alpha, s.beta)
        return x, y

    def reset(self) -> None:
        """Reset to prior (forget all observations)."""
        self._state = PosteriorState(
            dimension=self.dimension,
            alpha=self.prior_alpha,
            beta=self.prior_beta,
        )


class CompletenessScorer(_BaseDimensionScorer):
    """
    Tracks fraction of non-null values.

    successes = non-null row count
    total     = total row count
    """
    dimension = DQDimension.COMPLETENESS

    @staticmethod
    def from_dataframe(df) -> BatchObservation:
        """Convenience: create BatchObservation from a pandas DataFrame."""
        import pandas as pd
        total = len(df)
        successes = int(df.notna().all(axis=1).sum())
        return BatchObservation(
            dimension=DQDimension.COMPLETENESS,
            successes=successes,
            total=total,
            batch_id="",
            timestamp=datetime.now(timezone.utc),
        )


class FreshnessScorer(_BaseDimensionScorer):
    """
    Tracks fraction of records whose timestamp falls within an expected window.

    successes = rows with timestamp_col within now - max_age
    total     = total row count
    """
    dimension = DQDimension.FRESHNESS

    def __init__(
        self,
        max_age: timedelta = timedelta(hours=1),
        prior_alpha: float = 2.0,
        prior_beta: float = 2.0,
        health_threshold: float = 0.95,
    ):
        super().__init__(prior_alpha, prior_beta, health_threshold)
        self.max_age = max_age

    def from_dataframe(self, df, timestamp_col: str) -> BatchObservation:
        """Convenience: count rows fresher than max_age."""
        import pandas as pd

        now = datetime.now(timezone.utc)
        cutoff = now - self.max_age

        ts = pd.to_datetime(df[timestamp_col], utc=True)
        successes = int((ts >= cutoff).sum())
        return BatchObservation(
            dimension=DQDimension.FRESHNESS,
            successes=successes,
            total=len(df),
            batch_id="",
            timestamp=now,
        )


class UniquenessScorer(_BaseDimensionScorer):
    """
    Tracks fraction of distinct rows (or key-column combinations).

    successes = distinct row count
    total     = total row count
    """
    dimension = DQDimension.UNIQUENESS

    @staticmethod
    def from_dataframe(df, subset=None) -> BatchObservation:
        """Convenience: count unique rows over `subset` columns."""
        total = len(df)
        successes = int(df.drop_duplicates(subset=subset).shape[0])
        return BatchObservation(
            dimension=DQDimension.UNIQUENESS,
            successes=successes,
            total=total,
            batch_id="",
            timestamp=datetime.now(timezone.utc),
        )
