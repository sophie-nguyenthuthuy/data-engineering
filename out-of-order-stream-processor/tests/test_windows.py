"""Tests for windowing strategies."""
import pytest
from src.windows import TumblingWindow, SlidingWindow, SessionWindow
from src.windows.base import WindowAssignment


class TestTumblingWindow:
    def test_single_assignment(self):
        w = TumblingWindow(size_seconds=10)
        assigns = w.assign(15.0)
        assert len(assigns) == 1
        assert assigns[0] == WindowAssignment(10.0, 20.0)

    def test_boundary_at_start(self):
        w = TumblingWindow(size_seconds=10)
        assert w.assign(10.0)[0] == WindowAssignment(10.0, 20.0)

    def test_boundary_just_before_end(self):
        w = TumblingWindow(size_seconds=10)
        assert w.assign(19.999)[0] == WindowAssignment(10.0, 20.0)

    def test_offset(self):
        w = TumblingWindow(size_seconds=10, offset=3.0)
        assigns = w.assign(12.0)
        assert assigns[0].start == 3.0 or assigns[0].start == 13.0

    def test_not_session(self):
        assert not TumblingWindow(60).is_session_window()


class TestSlidingWindow:
    def test_event_in_multiple_windows(self):
        w = SlidingWindow(size_seconds=20, slide_seconds=10)
        assigns = w.assign(15.0)
        assert len(assigns) == 2
        starts = {a.start for a in assigns}
        assert 0.0 in starts
        assert 10.0 in starts

    def test_event_in_one_window_at_edge(self):
        w = SlidingWindow(size_seconds=10, slide_seconds=10)
        # Same as tumbling
        assigns = w.assign(5.0)
        assert len(assigns) == 1

    def test_slide_larger_than_size_raises(self):
        with pytest.raises(ValueError):
            SlidingWindow(size_seconds=5, slide_seconds=10)

    def test_coverage_count(self):
        w = SlidingWindow(size_seconds=30, slide_seconds=10)
        # event at t=25 should be in windows starting at 0, 10, 20
        assigns = w.assign(25.0)
        assert len(assigns) == 3


class TestSessionWindow:
    def test_single_event_provisional(self):
        w = SessionWindow(gap_seconds=30)
        assigns = w.assign(100.0)
        assert len(assigns) == 1
        assert assigns[0] == WindowAssignment(100.0, 130.0)

    def test_merge_overlapping(self):
        wins = [
            WindowAssignment(0, 30),
            WindowAssignment(20, 50),
            WindowAssignment(60, 90),
        ]
        merged = SessionWindow.merge(wins)
        assert len(merged) == 2
        assert merged[0] == WindowAssignment(0, 50)
        assert merged[1] == WindowAssignment(60, 90)

    def test_merge_touching(self):
        wins = [WindowAssignment(0, 30), WindowAssignment(30, 60)]
        merged = SessionWindow.merge(wins)
        assert len(merged) == 1
        assert merged[0] == WindowAssignment(0, 60)

    def test_merge_no_overlap(self):
        wins = [WindowAssignment(0, 10), WindowAssignment(20, 30)]
        merged = SessionWindow.merge(wins)
        assert len(merged) == 2

    def test_is_session(self):
        assert SessionWindow(gap_seconds=60).is_session_window()
