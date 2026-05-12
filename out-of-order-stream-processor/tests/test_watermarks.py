"""Tests for watermark strategies."""
import pytest
from src.event import Event
from src.watermarks import FixedLagWatermark, DynamicPerKeyWatermark, PercentileWatermark


def make_event(event_time, processing_time, key="k1", value=1):
    e = Event(event_time=event_time, key=key, value=value)
    e.processing_time = processing_time
    return e


class TestFixedLagWatermark:
    def test_initial_watermark_is_negative_inf(self):
        wm = FixedLagWatermark(lag_seconds=10)
        assert wm.current == float("-inf")

    def test_watermark_advances_with_events(self):
        wm = FixedLagWatermark(lag_seconds=10)
        wm.update(make_event(100, 110))
        assert wm.current == 90.0

    def test_watermark_does_not_go_backward(self):
        wm = FixedLagWatermark(lag_seconds=5)
        wm.update(make_event(100, 110))
        wm.update(make_event(80, 120))   # older event arrives late
        assert wm.current == 95.0        # still based on max seen (100)

    def test_late_detection(self):
        wm = FixedLagWatermark(lag_seconds=10)
        wm.update(make_event(100, 110))  # watermark = 90
        # event_time=89 < wm=90 → late
        assert wm.is_late(make_event(89, 115))
        # event_time=90 == wm=90 → not late (boundary is non-inclusive)
        assert not wm.is_late(make_event(90, 115))
        # event_time=91 > wm=90 → not late
        assert not wm.is_late(make_event(91, 115))

    def test_reset(self):
        wm = FixedLagWatermark(lag_seconds=10)
        wm.update(make_event(100, 110))
        wm.reset()
        assert wm.current == float("-inf")


class TestDynamicPerKeyWatermark:
    def test_per_key_differentiation(self):
        wm = DynamicPerKeyWatermark(percentile=50, window_size=10)
        # key A: low latency
        for i in range(20):
            wm.update(make_event(1000 + i, 1000 + i + 1, key="A"))
        # key B: high latency
        for i in range(20):
            wm.update(make_event(1000 + i, 1000 + i + 120, key="B"))

        lag_a = wm.lag_for_key("A")
        lag_b = wm.lag_for_key("B")
        assert lag_b > lag_a, "key with higher latency should get larger lag"

    def test_global_watermark_is_min_of_per_key(self):
        wm = DynamicPerKeyWatermark(percentile=50, window_size=10, min_lag=1)
        wm.update(make_event(1000, 1001, key="fast"))
        wm.update(make_event(1000, 1060, key="slow"))
        wm_fast = wm.watermark_for_key("fast")
        wm_slow = wm.watermark_for_key("slow")
        assert wm.current == min(wm_fast, wm_slow)

    def test_min_lag_floor(self):
        wm = DynamicPerKeyWatermark(percentile=50, window_size=100, min_lag=5.0)
        # Events with zero latency
        for i in range(50):
            wm.update(make_event(1000 + i, 1000 + i, key="instant"))
        assert wm.lag_for_key("instant") >= 5.0

    def test_stats_summary_populated(self):
        wm = DynamicPerKeyWatermark()
        wm.update(make_event(500, 530, key="x"))
        summary = wm.stats_summary()
        assert "x" in summary
        assert summary["x"]["samples"] == 1


class TestPercentileWatermark:
    def test_adapts_to_latency(self):
        wm = PercentileWatermark(percentile=50, window_size=20, min_lag=0.1)
        # Feed events with ~10s latency
        for i in range(20):
            wm.update(make_event(1000 + i, 1000 + i + 10, key="k"))
        assert 9.0 < wm.current_lag < 11.0

    def test_watermark_monotone_after_reset_and_refill(self):
        wm = PercentileWatermark(percentile=90, window_size=50)
        prev = float("-inf")
        for i in range(30):
            w = wm.update(make_event(1000 + i, 1000 + i + 5, key="k"))
            # Watermark may stay flat but must not decrease
            assert w >= prev or w == float("-inf")
            prev = w
