"""
BayesianDQScorer — top-level facade.

Usage::

    scorer = BayesianDQScorer()
    result = scorer.score_batch(
        batch_id="batch_001",
        observations={
            DQDimension.COMPLETENESS: BatchObservation(...),
            DQDimension.FRESHNESS:    BatchObservation(...),
            DQDimension.UNIQUENESS:   BatchObservation(...),
        }
    )
    print(result.summary())
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from .alerts import AlertManager
from .dimensions import CompletenessScorer, FreshnessScorer, UniquenessScorer
from .models import BatchObservation, BatchResult, DQDimension, PosteriorState


class BayesianDQScorer:
    """
    Orchestrates Bayesian quality scoring across all three dimensions.

    Parameters
    ----------
    completeness_prior, freshness_prior, uniqueness_prior:
        (alpha, beta) tuples for each dimension's Beta prior.
        Default Beta(2,2) — weakly informative, centered at 0.5.
    health_thresholds:
        Minimum quality rate considered "healthy" per dimension.
        Used to compute P(healthy) = P(rate > threshold).
    alert_thresholds:
        P(healthy) below which an AlertEvent fires per dimension.
    alert_cooldown:
        Number of batches to suppress repeat alerts for the same dimension.
    alert_handlers:
        Optional callables(AlertEvent) for custom notification sinks.
    """

    def __init__(
        self,
        completeness_prior: tuple[float, float] = (2.0, 2.0),
        freshness_prior: tuple[float, float] = (2.0, 2.0),
        uniqueness_prior: tuple[float, float] = (2.0, 2.0),
        health_thresholds: Optional[dict[DQDimension, float]] = None,
        alert_thresholds: Optional[dict[DQDimension, float]] = None,
        alert_cooldown: int = 3,
        alert_handlers=None,
    ):
        _ht = health_thresholds or {}

        self.scorers = {
            DQDimension.COMPLETENESS: CompletenessScorer(
                prior_alpha=completeness_prior[0],
                prior_beta=completeness_prior[1],
                health_threshold=_ht.get(DQDimension.COMPLETENESS, 0.95),
            ),
            DQDimension.FRESHNESS: FreshnessScorer(
                prior_alpha=freshness_prior[0],
                prior_beta=freshness_prior[1],
                health_threshold=_ht.get(DQDimension.FRESHNESS, 0.95),
            ),
            DQDimension.UNIQUENESS: UniquenessScorer(
                prior_alpha=uniqueness_prior[0],
                prior_beta=uniqueness_prior[1],
                health_threshold=_ht.get(DQDimension.UNIQUENESS, 0.99),
            ),
        }

        self.alert_manager = AlertManager(
            thresholds=alert_thresholds,
            cooldown_batches=alert_cooldown,
            handlers=alert_handlers or [],
        )

        self._results: list[BatchResult] = []

    def score_batch(
        self,
        batch_id: str,
        observations: dict[DQDimension, BatchObservation],
        timestamp: Optional[datetime] = None,
    ) -> BatchResult:
        """
        Ingest one batch, update all posteriors, evaluate alerts.

        Parameters
        ----------
        batch_id:   Unique identifier for this batch.
        observations: Map of dimension -> BatchObservation.
        timestamp:  Override batch timestamp (defaults to utcnow).
        """
        ts = timestamp or datetime.now(timezone.utc)

        # Stamp each observation with batch metadata
        for dim, obs in observations.items():
            obs.batch_id = batch_id
            if obs.timestamp is None:
                obs.timestamp = ts

        # Update posteriors
        posteriors: dict[DQDimension, PosteriorState] = {}
        for dim, obs in observations.items():
            posteriors[dim] = self.scorers[dim].observe(obs)

        # Compute P(healthy)
        p_healthy: dict[DQDimension, float] = {}
        for dim in observations:
            p_healthy[dim] = self.scorers[dim].p_healthy(posteriors[dim])

        # Evaluate alerts
        self.alert_manager.start_batch()
        alerts = []
        for dim in observations:
            event = self.alert_manager.evaluate(
                dimension=dim,
                p_healthy=p_healthy[dim],
                posterior=posteriors[dim],
                batch_id=batch_id,
            )
            if event:
                alerts.append(event)

        result = BatchResult(
            batch_id=batch_id,
            timestamp=ts,
            observations=list(observations.values()),
            posteriors=posteriors,
            p_healthy=p_healthy,
            alerts_fired=alerts,
        )
        self._results.append(result)
        return result

    @property
    def history(self) -> list[BatchResult]:
        return list(self._results)

    @property
    def current_posteriors(self) -> dict[DQDimension, PosteriorState]:
        return {dim: scorer.state for dim, scorer in self.scorers.items()}

    def credible_intervals(
        self, mass: float = 0.95
    ) -> dict[DQDimension, tuple[float, float]]:
        return {
            dim: scorer.credible_interval(mass)
            for dim, scorer in self.scorers.items()
        }

    def reset(self, dimension: Optional[DQDimension] = None) -> None:
        """Reset posterior(s) to prior — useful after a confirmed fix."""
        targets = [dimension] if dimension else list(self.scorers)
        for dim in targets:
            self.scorers[dim].reset()
        self._results.clear()
        self.alert_manager.clear_history()
