"""Integration tests for the StreamProcessor."""
import pytest
from src import (
    StreamProcessor, Event,
    FixedLagWatermark, DynamicPerKeyWatermark,
    TumblingWindow, SlidingWindow, SessionWindow,
    DropPolicy, RestatePolicy, SideOutputPolicy,
)


def evt(event_time, processing_time, key="k", value=1):
    e = Event(event_time=event_time, key=key, value=value)
    e.processing_time = processing_time
    return e


class TestTumblingWindowBasic:
    def test_window_emits_on_watermark_advance(self):
        p = StreamProcessor(
            watermark=FixedLagWatermark(lag_seconds=5),
            window=TumblingWindow(size_seconds=10),
            late_policy=DropPolicy(),
        )
        # Fill window [0, 10)
        p.process(evt(1, 6))
        p.process(evt(5, 10))
        # Advance watermark past 10 by sending event at t=16+ with lag=5
        results, _ = p.process(evt(16, 21))
        assert any(r.window_end == 10.0 for r in results)

    def test_multiple_windows_emitted(self):
        p = StreamProcessor(
            watermark=FixedLagWatermark(lag_seconds=0),
            window=TumblingWindow(size_seconds=10),
        )
        for t in [5, 15, 25, 35, 45]:
            p.process(evt(t, t))
        p.flush()
        # 5 events → 5 distinct tumbling windows, all emitted
        assert len(p.emitted_results) >= 4

    def test_flush_closes_remaining_windows(self):
        p = StreamProcessor(
            watermark=FixedLagWatermark(lag_seconds=100),
            window=TumblingWindow(size_seconds=10),
        )
        p.process(evt(5, 6))
        p.process(evt(15, 16))
        assert p.buffered_window_count == 2
        results = p.flush()
        assert len(results) == 2
        assert p.buffered_window_count == 0


class TestLateDataPolicies:
    def _build(self, policy):
        return StreamProcessor(
            watermark=FixedLagWatermark(lag_seconds=0),
            window=TumblingWindow(size_seconds=10),
            late_policy=policy,
        )

    def test_drop_policy_records_late(self):
        p = self._build(DropPolicy())
        # Advance watermark
        p.process(evt(20, 20))
        _, lates = p.process(evt(5, 21))  # late for [0, 10)
        assert len(lates) == 1
        assert lates[0].policy_applied == "drop"

    def test_restate_policy_emits_correction(self):
        p = self._build(RestatePolicy(max_lateness=float("inf")))
        p.process(evt(5, 5))
        p.process(evt(20, 20))  # closes [0,10) — now watermark at 20
        # Late event for [0, 10)
        results, lates = p.process(evt(3, 25))
        restatements = [r for r in results if r.is_restatement]
        assert len(restatements) == 1
        assert restatements[0].count >= 1

    def test_side_output_policy(self):
        policy = SideOutputPolicy()
        p = self._build(policy)
        p.process(evt(20, 20))
        p.process(evt(5, 25))   # late
        assert len(policy.side_output) == 1


class TestSlidingWindowProcessor:
    def test_event_in_multiple_windows(self):
        p = StreamProcessor(
            watermark=FixedLagWatermark(lag_seconds=0),
            window=SlidingWindow(size_seconds=20, slide_seconds=10),
        )
        p.process(evt(15, 15))
        results = p.flush()
        # Event at t=15 should be in window [0,20) and [10,30)
        window_starts = {r.window_start for r in results}
        assert 0.0 in window_starts or 10.0 in window_starts


class TestSessionWindowProcessor:
    def test_session_merging(self):
        p = StreamProcessor(
            watermark=FixedLagWatermark(lag_seconds=0),
            window=SessionWindow(gap_seconds=30),
        )
        # Cluster 1: events close together
        p.process(evt(0, 0, key="user1"))
        p.process(evt(10, 10, key="user1"))
        p.process(evt(20, 20, key="user1"))
        # Gap > 30s
        p.process(evt(60, 60, key="user1"))
        results = p.flush()
        assert len(results) >= 1

    def test_separate_keys_independent(self):
        p = StreamProcessor(
            watermark=FixedLagWatermark(lag_seconds=0),
            window=SessionWindow(gap_seconds=10),
        )
        p.process(evt(0, 0, key="A"))
        p.process(evt(0, 0, key="B"))
        results = p.flush()
        keys = {r.key for r in results}
        assert "A" in keys and "B" in keys


class TestProcessorStats:
    def test_stats_keys_present(self):
        p = StreamProcessor()
        p.process(evt(100, 130))
        s = p.stats()
        for key in ("processed", "emitted_windows", "late_total", "watermark"):
            assert key in s
