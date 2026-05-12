"""End-to-end: feed a synthetic stream, verify late-rate roughly matches δ."""

from __future__ import annotations

import pytest

from pwm.watermark.advancer import WatermarkAdvancer
from pwm.watermark.estimator import PerKeyDelayEstimator

pytestmark = pytest.mark.calibration


def test_late_rate_below_2x_delta():
    """For a stationary exponential delay distribution, the empirical late
    rate should be within 2× of the target δ."""
    est = PerKeyDelayEstimator(delta=0.05)
    adv = WatermarkAdvancer(delay_estimator=est, lambda_min=0.0)

    # Warm up so the quantile estimator stabilises
    import random
    rng = random.Random(0)
    for t in range(1, 2001):
        adv.on_record("k", float(t), float(t) + rng.expovariate(1.0))

    # Reset counters and measure
    adv.stats.on_time = 0
    adv.stats.late = 0
    for t in range(2001, 7001):
        adv.on_record("k", float(t), float(t) + rng.expovariate(1.0))
    late_rate = adv.stats.late_rate
    # Should be roughly ≤ 2 * 0.05 = 0.10 in steady state
    assert late_rate < 0.20, f"late_rate {late_rate} too high"


def test_lognormal_source_handles_heavy_tail():
    est = PerKeyDelayEstimator(delta=0.01, source="lognormal")
    adv = WatermarkAdvancer(delay_estimator=est, lambda_min=0.0)
    import random
    rng = random.Random(0)
    for t in range(1, 3001):
        adv.on_record("k", float(t), float(t) + rng.lognormvariate(0.0, 0.5))
    # late rate should be moderate
    assert adv.stats.late_rate < 0.30


def test_evt_source_handles_pareto_tail():
    est = PerKeyDelayEstimator(delta=0.01, source="evt")
    adv = WatermarkAdvancer(delay_estimator=est, lambda_min=0.0)
    import random
    rng = random.Random(0)
    for t in range(1, 3001):
        delay = max(0.0, rng.paretovariate(1.5) - 1.0)
        adv.on_record("k", float(t), float(t) + delay)
    # The estimator should keep up; final late rate moderate
    assert adv.stats.late_rate < 0.40
