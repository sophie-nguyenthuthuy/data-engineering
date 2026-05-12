import time
import pytest
from adaptive_microbatch.backpressure import BackpressureMonitor


def test_no_signals_returns_zero():
    bp = BackpressureMonitor()
    assert bp.current_level() == 0.0


def test_single_signal_reflected():
    bp = BackpressureMonitor()
    bp.push("worker-1", 0.7)
    assert 0.6 < bp.current_level() < 0.8


def test_clamped_above_one():
    bp = BackpressureMonitor()
    bp.push("w", 1.5)
    assert bp.current_level() <= 1.0


def test_clamped_below_zero():
    bp = BackpressureMonitor()
    bp.push("w", -0.3)
    assert bp.current_level() >= 0.0


def test_is_saturated():
    bp = BackpressureMonitor()
    bp.push("w", 0.95)
    assert bp.is_saturated(threshold=0.85)
    assert not bp.is_saturated(threshold=0.99)


def test_multiple_sources_averaged():
    bp = BackpressureMonitor()
    bp.push("a", 0.2)
    bp.push("b", 0.8)
    level = bp.current_level()
    # Should be somewhere between 0.2 and 0.8
    assert 0.2 < level < 0.8


def test_callback_fires_on_push():
    bp = BackpressureMonitor()
    received = []
    bp.on_pressure_change(lambda lvl: received.append(lvl))
    bp.push("w", 0.5)
    assert len(received) == 1
    assert 0.4 < received[0] < 0.6


def test_clear_resets_to_zero():
    bp = BackpressureMonitor()
    bp.push("w", 0.9)
    bp.clear()
    assert bp.current_level() == 0.0
