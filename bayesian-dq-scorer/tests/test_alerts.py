"""Tests for AlertManager."""

from datetime import datetime, timezone

import pytest

from bayesian_dq.alerts import AlertManager
from bayesian_dq.models import DQDimension, PosteriorState


def _state(dim, alpha=2.0, beta=2.0):
    return PosteriorState(dimension=dim, alpha=alpha, beta=beta, batch_count=1,
                         last_updated=datetime.now(timezone.utc))


class TestAlertManager:
    def test_fires_when_below_threshold(self):
        mgr = AlertManager(thresholds={DQDimension.COMPLETENESS: 0.50})
        mgr.start_batch()
        event = mgr.evaluate(DQDimension.COMPLETENESS, p_healthy=0.10,
                             posterior=_state(DQDimension.COMPLETENESS), batch_id="b001")
        assert event is not None
        assert event.dimension == DQDimension.COMPLETENESS

    def test_no_fire_when_above_threshold(self):
        mgr = AlertManager(thresholds={DQDimension.COMPLETENESS: 0.50})
        mgr.start_batch()
        event = mgr.evaluate(DQDimension.COMPLETENESS, p_healthy=0.80,
                             posterior=_state(DQDimension.COMPLETENESS), batch_id="b001")
        assert event is None

    def test_cooldown_suppresses_subsequent_alerts(self):
        mgr = AlertManager(thresholds={DQDimension.FRESHNESS: 0.50}, cooldown_batches=3)
        events = []
        for i in range(6):
            mgr.start_batch()
            e = mgr.evaluate(DQDimension.FRESHNESS, p_healthy=0.05,
                             posterior=_state(DQDimension.FRESHNESS), batch_id=f"b{i:03d}")
            if e:
                events.append(e)
        # Cooldown=3 → fires at batch 1 and 5 (indices 0 and 4 of loop)
        assert len(events) == 2

    def test_custom_handler_called(self):
        received = []
        mgr = AlertManager(
            thresholds={DQDimension.UNIQUENESS: 0.50},
            handlers=[received.append],
        )
        mgr.start_batch()
        mgr.evaluate(DQDimension.UNIQUENESS, p_healthy=0.10,
                     posterior=_state(DQDimension.UNIQUENESS), batch_id="b001")
        assert len(received) == 1

    def test_history_tracks_events(self):
        mgr = AlertManager(cooldown_batches=1)
        for i in range(4):
            mgr.start_batch()
            mgr.evaluate(DQDimension.COMPLETENESS, p_healthy=0.05,
                         posterior=_state(DQDimension.COMPLETENESS), batch_id=f"b{i:03d}")
        assert len(mgr.history) > 0

    def test_alert_event_dict_structure(self):
        mgr = AlertManager(thresholds={DQDimension.COMPLETENESS: 0.50})
        mgr.start_batch()
        event = mgr.evaluate(DQDimension.COMPLETENESS, p_healthy=0.10,
                             posterior=_state(DQDimension.COMPLETENESS), batch_id="b001")
        d = event.to_dict()
        for key in ("dimension", "batch_id", "timestamp", "p_healthy", "threshold", "message"):
            assert key in d
