"""Tests for late-data handling policies."""
import pytest
from src.event import Event
from src.windows.base import WindowAssignment
from src.policies import DropPolicy, RestatePolicy, SideOutputPolicy


def make_event(event_time, key="k", value=1):
    e = Event(event_time=event_time, key=key, value=value)
    e.processing_time = event_time + 5
    return e


WIN = WindowAssignment(0.0, 60.0)
WM = 100.0   # watermark well past the window


class TestDropPolicy:
    def test_returns_no_results(self):
        p = DropPolicy()
        results, lates = p.handle(make_event(10), WIN, [], WM)
        assert results == []
        assert len(lates) == 1

    def test_late_record_fields(self):
        p = DropPolicy()
        event = make_event(10)
        _, lates = p.handle(event, WIN, [], WM)
        le = lates[0]
        assert le.event is event
        assert le.policy_applied == "drop"
        assert le.watermark_at_arrival == WM


class TestRestatePolicy:
    def test_restatement_includes_all_events(self):
        p = RestatePolicy(max_lateness=float("inf"))
        existing = [make_event(20), make_event(30)]
        late = make_event(10)
        results, lates = p.handle(late, WIN, existing, WM)
        assert len(results) == 1
        r = results[0]
        assert r.is_restatement
        assert len(r.events) == 3
        assert r.events[0].event_time == 10.0  # sorted

    def test_drop_when_beyond_max_lateness(self):
        p = RestatePolicy(max_lateness=10.0)
        # watermark=100, event_time=10 → lateness=90 > 10
        results, lates = p.handle(make_event(10), WIN, [], WM)
        assert results == []
        assert "too_late" in lates[0].policy_applied

    def test_within_max_lateness(self):
        p = RestatePolicy(max_lateness=200.0)
        results, _ = p.handle(make_event(10), WIN, [], WM)
        assert len(results) == 1


class TestSideOutputPolicy:
    def test_no_results_emitted(self):
        p = SideOutputPolicy()
        results, lates = p.handle(make_event(10), WIN, [], WM)
        assert results == []
        assert len(lates) == 1

    def test_events_accumulate_in_side_output(self):
        p = SideOutputPolicy()
        for i in range(5):
            p.handle(make_event(i), WIN, [], WM)
        assert len(p.side_output) == 5

    def test_drain_clears_side_output(self):
        p = SideOutputPolicy()
        p.handle(make_event(10), WIN, [], WM)
        drained = p.drain_side_output()
        assert len(drained) == 1
        assert p.side_output == []
