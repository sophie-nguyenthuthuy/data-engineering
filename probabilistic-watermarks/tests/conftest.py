"""Shared fixtures."""

from __future__ import annotations

import pytest

from pwm.watermark.advancer import WatermarkAdvancer
from pwm.watermark.estimator import PerKeyDelayEstimator
from pwm.watermark.invariants import MonotonicityChecker


@pytest.fixture
def estimator() -> PerKeyDelayEstimator:
    return PerKeyDelayEstimator(delta=1e-3)


@pytest.fixture
def advancer(estimator: PerKeyDelayEstimator) -> WatermarkAdvancer:
    return WatermarkAdvancer(delay_estimator=estimator, lambda_min=0.0)


@pytest.fixture
def checker(advancer: WatermarkAdvancer) -> MonotonicityChecker:
    return MonotonicityChecker(advancer=advancer)
