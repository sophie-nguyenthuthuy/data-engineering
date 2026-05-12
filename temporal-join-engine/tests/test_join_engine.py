"""Tests for the core AS OF join engine (on-time events only)."""
import pytest
from temporal_join import AsOfJoinEngine, Event, JoinResult, STREAM_LEFT, STREAM_RIGHT


def left(key: str, t: int, **payload) -> Event:
    return Event(key=key, event_time=t, stream_id=STREAM_LEFT, payload=payload)


def right(key: str, t: int, **payload) -> Event:
    return Event(key=key, event_time=t, stream_id=STREAM_RIGHT, payload=payload)


class TestBasicJoin:
    def test_left_with_no_right(self):
        engine = AsOfJoinEngine(lookback_window=60_000)
        results = engine.process_event(left("u1", 1000))
        assert len(results) == 1
        assert results[0].left_event.event_time == 1000
        assert results[0].right_event is None
        assert not results[0].retraction

    def test_right_on_time_no_output(self):
        engine = AsOfJoinEngine(lookback_window=60_000)
        results = engine.process_event(right("u1", 500))
        assert results == []

    def test_right_then_left_matches(self):
        engine = AsOfJoinEngine(lookback_window=60_000)
        engine.process_event(right("u1", 500))
        results = engine.process_event(left("u1", 1000))
        assert len(results) == 1
        assert results[0].right_event.event_time == 500

    def test_left_then_right_no_retroactive_join(self):
        """Right events arriving after left do not trigger retroactive join (only corrections)."""
        engine = AsOfJoinEngine(lookback_window=60_000)
        results_left = engine.process_event(left("u1", 1000))
        results_right = engine.process_event(right("u1", 2000))
        assert results_left[0].right_event is None
        assert results_right == []  # on-time right — no correction output

    def test_as_of_semantics_picks_latest_right(self):
        """Of multiple right events before T_l, the one with the largest event_time is chosen."""
        engine = AsOfJoinEngine(lookback_window=60_000)
        engine.process_event(right("u1", 100))
        engine.process_event(right("u1", 300))
        engine.process_event(right("u1", 200))
        results = engine.process_event(left("u1", 1000))
        assert results[0].right_event.event_time == 300

    def test_lookback_window_excludes_old_right(self):
        engine = AsOfJoinEngine(lookback_window=500)
        engine.process_event(right("u1", 100))   # 900 ms before left — outside window
        engine.process_event(right("u1", 800))   # 200 ms before left — inside window
        results = engine.process_event(left("u1", 1000))
        assert results[0].right_event.event_time == 800

    def test_right_event_exactly_on_left_time(self):
        engine = AsOfJoinEngine(lookback_window=60_000)
        engine.process_event(right("u1", 1000))
        results = engine.process_event(left("u1", 1000))
        assert results[0].right_event.event_time == 1000

    def test_right_event_after_left_not_matched(self):
        """Right event at T > T_l must not be the AS OF match."""
        engine = AsOfJoinEngine(lookback_window=60_000)
        engine.process_event(right("u1", 2000))
        results = engine.process_event(left("u1", 1000))
        assert results[0].right_event is None


class TestMultipleKeys:
    def test_keys_are_isolated(self):
        engine = AsOfJoinEngine(lookback_window=60_000)
        engine.process_event(right("u1", 500))
        engine.process_event(right("u2", 600))
        r1 = engine.process_event(left("u1", 1000))
        r2 = engine.process_event(left("u2", 1000))
        assert r1[0].right_event.event_time == 500
        assert r2[0].right_event.event_time == 600

    def test_unknown_key_no_match(self):
        engine = AsOfJoinEngine(lookback_window=60_000)
        engine.process_event(right("u1", 500))
        results = engine.process_event(left("u2", 1000))
        assert results[0].right_event is None


class TestMultipleLeftEvents:
    def test_each_left_gets_its_own_result(self):
        engine = AsOfJoinEngine(lookback_window=60_000)
        engine.process_event(right("u1", 100))
        engine.process_event(right("u1", 500))
        r1 = engine.process_event(left("u1", 300))   # should match R@100
        r2 = engine.process_event(left("u1", 1000))  # should match R@500
        assert r1[0].right_event.event_time == 100
        assert r2[0].right_event.event_time == 500

    def test_right_inserted_between_lefts(self):
        engine = AsOfJoinEngine(lookback_window=60_000)
        r1 = engine.process_event(left("u1", 300))   # no match yet
        engine.process_event(right("u1", 200))        # on-time, inserted after L@300
        r2 = engine.process_event(left("u1", 1000))  # should see R@200
        assert r1[0].right_event is None
        assert r2[0].right_event.event_time == 200


class TestIrreparablyLateLeft:
    def test_left_below_watermark_dropped(self):
        engine = AsOfJoinEngine(
            lookback_window=60_000,
            left_lateness_bound=100,
        )
        engine.process_event(left("u1", 10_000))   # advances left watermark to 9900
        result = engine.process_event(left("u1", 50))  # event_time 50 < watermark 9900
        assert result == []


class TestWatermarkAPI:
    def test_advance_right_watermark(self):
        engine = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=1000)
        engine.advance_right_watermark(5000)
        assert engine.right_watermark == 5000

    def test_advance_left_watermark(self):
        engine = AsOfJoinEngine(lookback_window=60_000, left_lateness_bound=1000)
        engine.advance_left_watermark(5000)
        assert engine.left_watermark == 5000
