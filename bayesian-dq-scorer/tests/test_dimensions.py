"""Unit tests for individual dimension scorers."""

import pytest
from datetime import datetime, timezone

from bayesian_dq.dimensions import CompletenessScorer, FreshnessScorer, UniquenessScorer
from bayesian_dq.models import BatchObservation, DQDimension


def _obs(dim, successes, total, batch_id="test"):
    return BatchObservation(
        dimension=dim,
        successes=successes,
        total=total,
        batch_id=batch_id,
        timestamp=datetime.now(timezone.utc),
    )


class TestCompletenessScorer:
    def test_initial_state_is_prior(self):
        s = CompletenessScorer(prior_alpha=2.0, prior_beta=2.0)
        assert s.state.alpha == 2.0
        assert s.state.beta == 2.0

    def test_observe_updates_alpha_beta(self):
        s = CompletenessScorer(prior_alpha=1.0, prior_beta=1.0)
        s.observe(_obs(DQDimension.COMPLETENESS, successes=90, total=100))
        assert s.state.alpha == 1.0 + 90
        assert s.state.beta == 1.0 + 10
        assert s.state.batch_count == 1

    def test_perfect_data_high_p_healthy(self):
        s = CompletenessScorer(health_threshold=0.90)
        for _ in range(5):
            s.observe(_obs(DQDimension.COMPLETENESS, successes=1000, total=1000))
        assert s.p_healthy() > 0.99

    def test_bad_data_low_p_healthy(self):
        s = CompletenessScorer(health_threshold=0.90)
        for _ in range(5):
            s.observe(_obs(DQDimension.COMPLETENESS, successes=50, total=1000))
        assert s.p_healthy() < 0.01

    def test_credible_interval_contains_mean(self):
        s = CompletenessScorer()
        s.observe(_obs(DQDimension.COMPLETENESS, successes=800, total=1000))
        lo, hi = s.credible_interval(0.95)
        assert lo < s.state.mean < hi

    def test_reset_restores_prior(self):
        s = CompletenessScorer(prior_alpha=3.0, prior_beta=7.0)
        s.observe(_obs(DQDimension.COMPLETENESS, successes=500, total=1000))
        s.reset()
        assert s.state.alpha == 3.0
        assert s.state.beta == 7.0
        assert s.state.batch_count == 0

    def test_pdf_curve_nonnegative(self):
        s = CompletenessScorer()
        x, y = s.pdf_curve()
        assert len(x) == len(y)
        assert (y >= 0).all()

    def test_zero_successes(self):
        s = CompletenessScorer(prior_alpha=1.0, prior_beta=1.0)
        s.observe(_obs(DQDimension.COMPLETENESS, successes=0, total=100))
        assert s.state.alpha == 1.0
        assert s.state.beta == 101.0

    def test_posterior_mean_bounded(self):
        s = CompletenessScorer()
        s.observe(_obs(DQDimension.COMPLETENESS, successes=700, total=1000))
        assert 0 < s.state.mean < 1


class TestUniquenessScorer:
    def test_duplicate_heavy_batch_degrades_posterior(self):
        s = UniquenessScorer(health_threshold=0.95)
        for _ in range(10):
            s.observe(_obs(DQDimension.UNIQUENESS, successes=700, total=1000))
        assert s.p_healthy() < 0.05

    def test_all_unique_healthy(self):
        s = UniquenessScorer(health_threshold=0.95)
        for _ in range(5):
            s.observe(_obs(DQDimension.UNIQUENESS, successes=1000, total=1000))
        assert s.p_healthy() > 0.99


class TestFreshnessScorer:
    def test_stale_data_low_p_healthy(self):
        s = FreshnessScorer(health_threshold=0.90)
        for _ in range(5):
            s.observe(_obs(DQDimension.FRESHNESS, successes=10, total=1000))
        assert s.p_healthy() < 0.01

    def test_fresh_data_healthy(self):
        s = FreshnessScorer(health_threshold=0.90)
        for _ in range(5):
            s.observe(_obs(DQDimension.FRESHNESS, successes=990, total=1000))
        assert s.p_healthy() > 0.99
