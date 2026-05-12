"""Monotonicity invariant checker."""

from __future__ import annotations

import random

import pytest

from pwm.watermark.advancer import WatermarkAdvancer
from pwm.watermark.estimator import PerKeyDelayEstimator
from pwm.watermark.invariants import MonotonicityChecker, MonotonicityViolation


def test_checker_passes_for_correct_stream(checker):
    rng = random.Random(0)
    for t in range(1, 2001):
        checker.check("k", float(t), float(t) + rng.expovariate(1.0))
    assert checker.violations == []


def test_checker_detects_watermark_regression():
    """Build a checker over a deliberately-broken advancer that lets the
    watermark go backwards. The checker must flag this."""
    est = PerKeyDelayEstimator(delta=1e-3)
    adv = WatermarkAdvancer(delay_estimator=est)
    ch = MonotonicityChecker(advancer=adv, strict=False)
    # Establish a high watermark
    for t in range(1, 501):
        ch.check("k", float(t), float(t) + 0.1)
    # Manually rewind the advancer's internal _w to simulate a bug
    adv._w = 0.0
    # Next check: the advancer will compute some w; if it ends up below the
    # tracked _last_w, the checker fires.
    ch.check("k", 1.0, 1.0)
    assert any("watermark" in v for v in ch.violations), \
        f"expected violation, got {ch.violations}"


def test_strict_mode_raises():
    est = PerKeyDelayEstimator(delta=1e-3)
    adv = WatermarkAdvancer(delay_estimator=est)
    ch = MonotonicityChecker(advancer=adv, strict=True)
    for t in range(1, 200):
        ch.check("k", float(t), float(t) + 0.1)
    # Rewind the advancer's _w to break monotonicity
    adv._w = 0.0
    with pytest.raises(MonotonicityViolation):
        ch.check("k", 1.0, 1.0)
