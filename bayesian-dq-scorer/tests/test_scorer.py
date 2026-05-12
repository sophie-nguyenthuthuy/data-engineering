"""Integration tests for BayesianDQScorer."""

import pytest
from datetime import datetime, timezone

from bayesian_dq import BayesianDQScorer
from bayesian_dq.models import BatchObservation, DQDimension


def _obs(dim, successes, total, batch_id="b001"):
    return BatchObservation(
        dimension=dim,
        successes=successes,
        total=total,
        batch_id=batch_id,
        timestamp=datetime.now(timezone.utc),
    )


def _healthy_batch(batch_id="b001"):
    return {
        DQDimension.COMPLETENESS: _obs(DQDimension.COMPLETENESS, 990, 1000, batch_id),
        DQDimension.FRESHNESS:    _obs(DQDimension.FRESHNESS,    980, 1000, batch_id),
        DQDimension.UNIQUENESS:   _obs(DQDimension.UNIQUENESS,   999, 1000, batch_id),
    }


def _degraded_batch(batch_id="b001"):
    return {
        DQDimension.COMPLETENESS: _obs(DQDimension.COMPLETENESS, 100, 1000, batch_id),
        DQDimension.FRESHNESS:    _obs(DQDimension.FRESHNESS,     50, 1000, batch_id),
        DQDimension.UNIQUENESS:   _obs(DQDimension.UNIQUENESS,   500, 1000, batch_id),
    }


class TestBayesianDQScorer:
    def test_healthy_batches_no_alert(self):
        scorer = BayesianDQScorer()
        for i in range(5):
            result = scorer.score_batch(f"b{i:03d}", _healthy_batch(f"b{i:03d}"))
        assert result.alerts_fired == []

    def test_degraded_batches_fire_alerts(self):
        scorer = BayesianDQScorer(alert_cooldown=1)
        all_alerts = []
        for i in range(10):
            result = scorer.score_batch(f"b{i:03d}", _degraded_batch(f"b{i:03d}"))
            all_alerts.extend(result.alerts_fired)
        assert len(all_alerts) > 0

    def test_result_has_all_dimensions(self):
        scorer = BayesianDQScorer()
        result = scorer.score_batch("b000", _healthy_batch())
        assert DQDimension.COMPLETENESS in result.p_healthy
        assert DQDimension.FRESHNESS in result.p_healthy
        assert DQDimension.UNIQUENESS in result.p_healthy

    def test_p_healthy_bounds(self):
        scorer = BayesianDQScorer()
        result = scorer.score_batch("b000", _healthy_batch())
        for dim, ph in result.p_healthy.items():
            assert 0.0 <= ph <= 1.0, f"{dim}: {ph}"

    def test_history_accumulates(self):
        scorer = BayesianDQScorer()
        for i in range(7):
            scorer.score_batch(f"b{i:03d}", _healthy_batch(f"b{i:03d}"))
        assert len(scorer.history) == 7

    def test_reset_clears_history_and_posteriors(self):
        scorer = BayesianDQScorer(
            completeness_prior=(2.0, 2.0),
        )
        for i in range(5):
            scorer.score_batch(f"b{i:03d}", _degraded_batch(f"b{i:03d}"))
        scorer.reset()
        assert len(scorer.history) == 0
        # Posterior should be back at prior
        post = scorer.current_posteriors[DQDimension.COMPLETENESS]
        assert post.alpha == 2.0
        assert post.beta == 2.0

    def test_cooldown_suppresses_repeat_alerts(self):
        scorer = BayesianDQScorer(alert_cooldown=5)
        events = []
        for i in range(10):
            result = scorer.score_batch(f"b{i:03d}", _degraded_batch(f"b{i:03d}"))
            events.extend(result.alerts_fired)
        # With cooldown=5 and 10 batches, at most 2 alerts per dimension
        completeness_alerts = [e for e in events if e.dimension == DQDimension.COMPLETENESS]
        assert len(completeness_alerts) <= 2

    def test_summary_keys(self):
        scorer = BayesianDQScorer()
        result = scorer.score_batch("b000", _healthy_batch())
        s = result.summary()
        assert "batch_id" in s
        assert "timestamp" in s
        assert "dimensions" in s
        assert "alerts_fired" in s

    def test_credible_intervals_contain_posterior_mean(self):
        scorer = BayesianDQScorer()
        for i in range(3):
            scorer.score_batch(f"b{i:03d}", _healthy_batch(f"b{i:03d}"))
        for dim, (lo, hi) in scorer.credible_intervals().items():
            mean = scorer.current_posteriors[dim].mean
            assert lo < mean < hi, f"{dim}: mean={mean}, CI=[{lo},{hi}]"

    def test_custom_health_thresholds(self):
        scorer = BayesianDQScorer(
            health_thresholds={DQDimension.COMPLETENESS: 0.50},
        )
        result = scorer.score_batch("b000", {
            DQDimension.COMPLETENESS: _obs(DQDimension.COMPLETENESS, 600, 1000),
        })
        # 60% completeness >> 50% threshold => high P(healthy) after 1 batch
        assert result.p_healthy[DQDimension.COMPLETENESS] > 0.5
