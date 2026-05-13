"""Lognormal forecaster + CUSUM drift tests."""

from __future__ import annotations

import math
import random

import pytest

from fps.forecast.cusum import CUSUMDetector
from fps.forecast.lognormal import LognormalForecaster, TaskStats, _ndtri


def _sample_lognormal(rng: random.Random, mu: float, sigma: float) -> float:
    return math.exp(mu + sigma * rng.gauss(0.0, 1.0))


def test_taskstats_starts_empty():
    s = TaskStats()
    assert s.n == 0
    assert s.mu == 0.0 and s.sigma == 0.0


def test_taskstats_ignores_non_positive():
    s = TaskStats()
    s.add(0.0)
    s.add(-1.0)
    assert s.n == 0


def test_taskstats_quantile_zero_with_no_data():
    assert TaskStats().quantile(0.5) == 0.0


def test_taskstats_quantile_rejects_bad_q():
    s = TaskStats()
    with pytest.raises(ValueError):
        s.quantile(0.0)
    with pytest.raises(ValueError):
        s.quantile(1.0)


def test_ndtri_known_values():
    assert abs(_ndtri(0.975) - 1.96) < 0.005
    assert abs(_ndtri(0.5)) < 1e-6
    assert abs(_ndtri(0.025) + 1.96) < 0.005


def test_ndtri_monotone():
    assert _ndtri(0.3) < _ndtri(0.5) < _ndtri(0.7)


def test_forecaster_rejects_empty_task():
    f = LognormalForecaster()
    with pytest.raises(ValueError):
        f.observe("", 1.0)


def test_forecaster_returns_default_when_unknown():
    f = LognormalForecaster()
    assert f.mean("unseen", default=42.0) == 42.0
    assert f.p95("unseen", default=99.0) == 99.0


def test_forecaster_recovers_mu_sigma_from_lognormal_stream():
    rng = random.Random(0)
    f = LognormalForecaster()
    for _ in range(2_000):
        f.observe("t1", _sample_lognormal(rng, mu=2.0, sigma=0.5))
    s = f.stats("t1")
    assert abs(s.mu - 2.0) < 0.05
    assert abs(s.sigma - 0.5) < 0.05


def test_forecaster_mean_matches_lognormal_formula():
    rng = random.Random(1)
    f = LognormalForecaster()
    for _ in range(2_000):
        f.observe("t1", _sample_lognormal(rng, mu=2.0, sigma=0.5))
    # E[X] = exp(mu + sigma²/2) = exp(2.125) ≈ 8.367
    assert abs(f.mean("t1") - math.exp(2.0 + 0.5 * 0.5**2)) < 0.5


def test_forecaster_p95_matches_lognormal_formula():
    rng = random.Random(2)
    f = LognormalForecaster()
    for _ in range(3_000):
        f.observe("t1", _sample_lognormal(rng, mu=2.0, sigma=0.5))
    truth = math.exp(2.0 + 0.5 * _ndtri(0.95))
    assert abs(f.p95("t1") - truth) / truth < 0.10


def test_forecaster_reset_clears_a_task():
    f = LognormalForecaster()
    f.observe("t", 1.0)
    f.observe("t", 2.0)
    f.reset("t")
    assert f.stats("t").n == 0


# ----------------------------------------------------------------- CUSUM


def test_cusum_rejects_bad_params():
    with pytest.raises(ValueError):
        CUSUMDetector(mean=0.0, sigma=0.0)
    with pytest.raises(ValueError):
        CUSUMDetector(mean=0.0, sigma=1.0, h=0.0)
    with pytest.raises(ValueError):
        CUSUMDetector(mean=0.0, sigma=1.0, k=-1.0)


def test_cusum_does_not_fire_on_in_distribution_samples():
    rng = random.Random(0)
    d = CUSUMDetector(mean=0.0, sigma=1.0, k=0.5, h=5.0)
    for _ in range(500):
        d.update(rng.gauss(0.0, 1.0))
    assert not d.fired


def test_cusum_fires_on_persistent_upward_drift():
    rng = random.Random(0)
    d = CUSUMDetector(mean=0.0, sigma=1.0, k=0.5, h=5.0)
    for _ in range(60):
        d.update(rng.gauss(2.0, 1.0))  # shifted up by 2σ
    assert d.fired


def test_cusum_fires_on_persistent_downward_drift():
    rng = random.Random(0)
    d = CUSUMDetector(mean=0.0, sigma=1.0, k=0.5, h=5.0)
    for _ in range(60):
        d.update(rng.gauss(-2.0, 1.0))
    assert d.fired


def test_cusum_reset_clears_state():
    d = CUSUMDetector(mean=0.0, sigma=1.0, k=0.5, h=2.0)
    for _ in range(20):
        d.update(5.0)
    assert d.fired
    d.reset()
    assert not d.fired and d.s_pos == 0.0 and d.s_neg == 0.0 and d.n == 0
