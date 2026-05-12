"""Tests for time-window utilities."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from replay.engine.window import parse_window, split_window, window_from_days_ago
from replay.models import TimeWindow


class TestWindowFromDaysAgo:
    def test_basic(self):
        w = window_from_days_ago(7)
        delta = w.end - w.start
        assert delta.days == 7

    def test_max_30_days(self):
        w = window_from_days_ago(30)
        assert (w.end - w.start).days == 30

    def test_exceeds_max_raises(self):
        with pytest.raises(ValueError, match="30"):
            window_from_days_ago(31)

    def test_zero_raises(self):
        with pytest.raises(ValueError):
            window_from_days_ago(0)

    def test_timezone_aware(self):
        w = window_from_days_ago(1)
        assert w.start.tzinfo is not None
        assert w.end.tzinfo is not None


class TestParseWindow:
    def test_iso_strings(self):
        w = parse_window("2024-03-01T00:00:00Z", "2024-03-07T23:59:59Z")
        assert w.start.year == 2024
        assert w.start.month == 3
        assert w.end.day == 7

    def test_naive_strings_get_utc(self):
        w = parse_window("2024-03-01 00:00:00", "2024-03-02 00:00:00")
        assert w.start.tzinfo is not None

    def test_end_before_start_raises(self):
        with pytest.raises(ValueError):
            parse_window("2024-03-07T00:00:00Z", "2024-03-01T00:00:00Z")

    def test_over_30_days_raises(self):
        with pytest.raises(ValueError, match="30 days"):
            parse_window("2024-01-01T00:00:00Z", "2024-02-15T00:00:00Z")


class TestSplitWindow:
    def test_splits_into_days(self):
        w = parse_window("2024-03-01T00:00:00Z", "2024-03-03T00:00:00Z")
        chunks = split_window(w, chunk_hours=24)
        assert len(chunks) == 2
        for chunk in chunks:
            assert (chunk.end - chunk.start).total_seconds() <= 24 * 3600 + 1

    def test_last_chunk_ends_at_window_end(self):
        w = parse_window("2024-03-01T00:00:00Z", "2024-03-01T06:00:00Z")
        chunks = split_window(w, chunk_hours=4)
        assert chunks[-1].end == w.end

    def test_single_chunk_if_smaller_than_interval(self):
        w = parse_window("2024-03-01T00:00:00Z", "2024-03-01T02:00:00Z")
        chunks = split_window(w, chunk_hours=24)
        assert len(chunks) == 1


class TestTimeWindowContains:
    def test_contains_inside(self):
        w = TimeWindow(
            start=datetime(2024, 3, 14, tzinfo=timezone.utc),
            end=datetime(2024, 3, 15, tzinfo=timezone.utc),
        )
        ts = datetime(2024, 3, 14, 12, 0, tzinfo=timezone.utc)
        assert w.contains(ts)

    def test_contains_exclusive_boundary(self):
        w = TimeWindow(
            start=datetime(2024, 3, 14, tzinfo=timezone.utc),
            end=datetime(2024, 3, 15, tzinfo=timezone.utc),
        )
        assert w.contains(w.start)
        assert w.contains(w.end)

    def test_does_not_contain_outside(self):
        w = TimeWindow(
            start=datetime(2024, 3, 14, tzinfo=timezone.utc),
            end=datetime(2024, 3, 15, tzinfo=timezone.utc),
        )
        assert not w.contains(datetime(2024, 3, 13, tzinfo=timezone.utc))
        assert not w.contains(datetime(2024, 3, 16, tzinfo=timezone.utc))
