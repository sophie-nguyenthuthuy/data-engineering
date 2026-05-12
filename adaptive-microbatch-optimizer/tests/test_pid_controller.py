import pytest
from adaptive_microbatch.pid_controller import PIDConfig, PIDController


def test_positive_error_shrinks_window():
    pid = PIDController()
    w0 = 1.0
    w1 = pid.apply(w0, error=0.8)   # high latency → shrink
    assert w1 < w0


def test_negative_error_grows_window():
    pid = PIDController()
    w0 = 0.5
    w1 = pid.apply(w0, error=-0.8)  # latency way below target → grow
    assert w1 > w0


def test_output_clamped_to_min():
    cfg = PIDConfig(kp=10.0, ki=0.0, kd=0.0)
    pid = PIDController(cfg)
    w = 0.1
    for _ in range(50):
        w = pid.apply(w, error=1.0)
    assert w == pytest.approx(cfg.min_output, abs=1e-9)


def test_output_clamped_to_max():
    cfg = PIDConfig(kp=10.0, ki=0.0, kd=0.0)
    pid = PIDController(cfg)
    w = 4.0
    for _ in range(50):
        w = pid.apply(w, error=-1.0)
    assert w == pytest.approx(cfg.max_output, abs=1e-9)


def test_zero_error_is_stable():
    pid = PIDController()
    w = 1.0
    for _ in range(20):
        w_new = pid.apply(w, error=0.0)
        # Derivative term may cause tiny transients; check convergence
    assert abs(w_new - w) < 0.01


def test_anti_windup_limits_integral():
    cfg = PIDConfig(kp=0.0, ki=1.0, kd=0.0, integral_clamp=0.5)
    pid = PIDController(cfg)
    w = 2.0
    for _ in range(1000):
        pid.apply(w, error=1.0)
    assert abs(pid._integral) <= cfg.integral_clamp + 1e-6


def test_reset_clears_state():
    pid = PIDController()
    pid.apply(1.0, error=0.9)
    pid.reset()
    assert pid._integral == 0.0
    assert pid._prev_error == 0.0
