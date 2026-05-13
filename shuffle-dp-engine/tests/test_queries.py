"""Private queries."""

from __future__ import annotations

import numpy as np

from sdp.local.randomizers import LocalConfig
from sdp.queries.histogram import private_histogram, private_mean


def test_histogram_recovers_distribution_at_high_eps():
    rng = np.random.default_rng(0)
    cfg = LocalConfig(eps0=4.0, domain_size=4)
    true_pmf = [0.5, 0.2, 0.2, 0.1]
    samples = list(rng.choice(4, size=20_000, p=true_pmf))
    est = private_histogram(samples, cfg, rng=rng)
    for t, e in zip(true_pmf, est, strict=False):
        assert abs(t - e) < 0.05


def test_histogram_sums_to_one():
    rng = np.random.default_rng(1)
    cfg = LocalConfig(eps0=2.0, domain_size=5)
    samples = list(rng.choice(5, size=5000))
    est = private_histogram(samples, cfg, rng=rng)
    assert abs(sum(est) - 1.0) < 1e-9


def test_histogram_empty_input():
    cfg = LocalConfig(eps0=1.0, domain_size=4)
    out = private_histogram([], cfg)
    assert out == [0.0] * 4


def test_mean_close_to_truth():
    rng = np.random.default_rng(0)
    values = list(rng.uniform(0, 100, size=5000))
    est = private_mean(values, lo=0.0, hi=100.0, eps=1.0, rng=rng)
    true_mean = sum(values) / len(values)
    assert abs(est - true_mean) < 5


def test_mean_empty_returns_zero():
    assert private_mean([], lo=0, hi=1, eps=1.0) == 0.0
