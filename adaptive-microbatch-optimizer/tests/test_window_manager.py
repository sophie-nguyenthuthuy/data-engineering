import pytest
from adaptive_microbatch.window_manager import AdaptiveWindowManager, SLAConfig
from adaptive_microbatch.pid_controller import PIDConfig


def _make_mgr(target_latency=0.1) -> AdaptiveWindowManager:
    sla = SLAConfig(target_latency_s=target_latency, backpressure_weight=0.5)
    pid = PIDConfig(kp=0.3, ki=0.02, kd=0.1)
    return AdaptiveWindowManager(sla=sla, pid_config=pid, initial_window=0.5)


def test_high_latency_shrinks_window():
    mgr = _make_mgr(target_latency=0.1)
    w0 = mgr.current_window
    # Feed 10 batches well above SLA
    for _ in range(10):
        mgr.after_batch(batch_size=100, processing_time_s=0.5)
    assert mgr.current_window < w0


def test_low_latency_grows_window():
    mgr = _make_mgr(target_latency=0.5)
    w0 = mgr.current_window
    for _ in range(10):
        mgr.after_batch(batch_size=100, processing_time_s=0.01)
    assert mgr.current_window > w0


def test_window_never_below_min():
    mgr = _make_mgr(target_latency=0.01)
    for _ in range(100):
        mgr.after_batch(batch_size=1, processing_time_s=10.0)
    assert mgr.current_window >= AdaptiveWindowManager.MIN_WINDOW


def test_window_never_above_max():
    mgr = _make_mgr(target_latency=100.0)
    for _ in range(100):
        mgr.after_batch(batch_size=1, processing_time_s=0.001)
    assert mgr.current_window <= AdaptiveWindowManager.MAX_WINDOW


def test_backpressure_shrinks_window():
    # Use a target latency equal to processing time so latency error ~= 0,
    # leaving only the backpressure component to drive the window down.
    mgr = _make_mgr(target_latency=0.01)
    mgr.backpressure.push("db", 0.95)
    w0 = mgr.current_window
    for _ in range(10):
        mgr.after_batch(batch_size=50, processing_time_s=0.01)
    assert mgr.current_window < w0


def test_history_recorded():
    mgr = _make_mgr()
    assert len(mgr.history()) == 0
    mgr.after_batch(10, 0.05)
    mgr.after_batch(10, 0.05)
    assert len(mgr.history()) == 2


def test_reset_restores_defaults():
    mgr = _make_mgr()
    for _ in range(20):
        mgr.after_batch(100, 0.5)
    mgr.reset()
    assert mgr.current_window == 0.5
    assert len(mgr.history()) == 0
