"""Tests for the Lambda speed layer incremental update logic."""

from __future__ import annotations

import uuid
from datetime import datetime

import pytest

from src.lambda_arch.models import Event
from src.lambda_arch.speed_layer import SpeedLayer


def _evt(etype: str, amount: float, user: str = "u1", hour: int = 10) -> Event:
    return Event(
        event_id=str(uuid.uuid4()),
        user_id=user,
        event_type=etype,
        amount=amount,
        timestamp=datetime(2024, 1, 3, hour, 0, 0),
    )


class TestSpeedLayerIncrementalUpdate:
    def test_single_event_increments_hourly_count(self) -> None:
        sl = SpeedLayer(local_mode=True)
        ev = _evt("purchase", 100.0)
        sl.process_event(ev)
        counts = sl.view.hourly_event_counts.data
        assert counts["2024-01-03T10"]["purchase"] == 1

    def test_multiple_events_same_hour(self) -> None:
        sl = SpeedLayer(local_mode=True)
        for _ in range(5):
            sl.process_event(_evt("click", 0.0))
        counts = sl.view.hourly_event_counts.data
        assert counts["2024-01-03T10"]["click"] == 5

    def test_events_different_hours_separate_buckets(self) -> None:
        sl = SpeedLayer(local_mode=True)
        sl.process_event(_evt("view", 0.0, hour=8))
        sl.process_event(_evt("view", 0.0, hour=9))
        counts = sl.view.hourly_event_counts.data
        assert counts["2024-01-03T08"]["view"] == 1
        assert counts["2024-01-03T09"]["view"] == 1

    def test_user_totals_updated(self) -> None:
        sl = SpeedLayer(local_mode=True)
        sl.process_event(_evt("purchase", 50.0, user="alice"))
        sl.process_event(_evt("purchase", 75.0, user="alice"))
        ut = sl.view.user_totals.data
        assert abs(float(ut["alice"]["total_amount"]) - 125.0) < 1e-6
        assert int(ut["alice"]["event_count"]) == 2

    def test_event_type_summary_avg_updates(self) -> None:
        sl = SpeedLayer(local_mode=True)
        sl.process_event(_evt("purchase", 100.0))
        sl.process_event(_evt("purchase", 200.0))
        summary = sl.view.event_type_summary.data["purchase"]
        assert int(summary["count"]) == 2
        assert abs(float(summary["avg_amount"]) - 150.0) < 1e-6

    def test_process_batch_of_events(self, sample_events: list[Event]) -> None:
        sl = SpeedLayer(local_mode=True)
        sl.process_events(sample_events)
        view = sl.get_view()
        total = sum(
            int(v["event_count"]) for v in view.user_totals.data.values()
        )
        assert total == len(sample_events)

    def test_view_is_mutable_reference(self) -> None:
        """get_view() should return the live view, not a copy."""
        sl = SpeedLayer(local_mode=True)
        view = sl.get_view()
        sl.process_event(_evt("signup", 0.0))
        assert "signup" in str(view.event_type_summary.data)
