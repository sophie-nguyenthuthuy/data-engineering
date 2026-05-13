"""Local randomizers."""

from __future__ import annotations

import math

import numpy as np
import pytest

from sdp.local.randomizers import (
    LocalConfig,
    gaussian_noise,
    laplace_noise,
    randomized_response,
)


def test_local_config_validates():
    with pytest.raises(ValueError):
        LocalConfig(eps0=0, domain_size=4)
    with pytest.raises(ValueError):
        LocalConfig(eps0=1.0, domain_size=1)


def test_rr_rejects_out_of_domain():
    cfg = LocalConfig(eps0=1.0, domain_size=4)
    with pytest.raises(ValueError):
        randomized_response(99, cfg)


def test_rr_marginal_close_to_truth_at_high_eps():
    cfg = LocalConfig(eps0=5.0, domain_size=4)
    rng = np.random.default_rng(0)
    correct = sum(1 for _ in range(5000) if randomized_response(0, cfg, rng) == 0)
    p_correct = correct / 5000
    p_true = math.exp(5.0) / (math.exp(5.0) + 3)
    assert abs(p_correct - p_true) < 0.03


def test_laplace_unbiased():
    rng = np.random.default_rng(42)
    samples = [laplace_noise(100.0, sensitivity=1.0, eps=1.0, rng=rng) for _ in range(50_000)]
    assert abs(np.mean(samples) - 100.0) < 0.5


def test_laplace_rejects_bad_args():
    with pytest.raises(ValueError):
        laplace_noise(0.0, sensitivity=1.0, eps=0)
    with pytest.raises(ValueError):
        laplace_noise(0.0, sensitivity=-1.0, eps=1.0)


def test_gaussian_unbiased():
    rng = np.random.default_rng(7)
    samples = [
        gaussian_noise(50.0, sensitivity=1.0, eps=0.5, delta=1e-5, rng=rng) for _ in range(20_000)
    ]
    assert abs(np.mean(samples) - 50.0) < 0.5


def test_gaussian_constraints():
    with pytest.raises(ValueError):
        gaussian_noise(0.0, sensitivity=1.0, eps=1.5, delta=1e-5)
    with pytest.raises(ValueError):
        gaussian_noise(0.0, sensitivity=1.0, eps=0.5, delta=0)
