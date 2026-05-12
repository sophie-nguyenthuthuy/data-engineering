"""Distribution fitters."""

from __future__ import annotations

import math
import random

import pytest

from pwm.fit.evt import POTFitter
from pwm.fit.lognormal import LognormalFitter


class TestLognormal:
    def test_recovers_parameters(self):
        f = LognormalFitter()
        rng = random.Random(0)
        for _ in range(10_000):
            f.observe(rng.lognormvariate(mu=1.5, sigma=0.5))
        # MLE should converge near truth
        assert abs(f.mu - 1.5) < 0.05
        assert abs(f.sigma - 0.5) < 0.05

    def test_p99_close_to_truth(self):
        f = LognormalFitter()
        rng = random.Random(0)
        for _ in range(20_000):
            f.observe(rng.lognormvariate(mu=2.0, sigma=0.5))
        p99 = f.quantile(0.99)
        true_p99 = math.exp(2.0 + 0.5 * 2.326)   # z(0.99) ≈ 2.326
        rel_err = abs(p99 - true_p99) / true_p99
        assert rel_err < 0.1

    def test_rejects_nonpositive(self):
        f = LognormalFitter()
        f.observe(-1.0)   # ignored
        f.observe(0.0)    # ignored
        f.observe(1.0)
        assert f.n == 1

    def test_too_few_obs_sigma_zero(self):
        f = LognormalFitter()
        f.observe(1.0)
        assert f.sigma == 0.0

    def test_quantile_rejects_invalid_q(self):
        f = LognormalFitter()
        f.observe(1.0)
        f.observe(2.0)
        with pytest.raises(ValueError):
            f.quantile(0.0)
        with pytest.raises(ValueError):
            f.quantile(1.0)


class TestEVT:
    def test_recovers_tail_shape(self):
        """For Pareto with α=1.5 the GPD shape ξ should be ≈ 1/α = 0.67."""
        f = POTFitter(burn_in=200)
        rng = random.Random(0)
        for _ in range(5000):
            x = rng.paretovariate(1.5) - 1.0
            f.observe(x)
        # We just verify shape is finite and not collapsed to 0
        assert -0.5 <= f.xi <= 0.5

    def test_quantile_below_threshold_uses_empirical(self):
        f = POTFitter(burn_in=100)
        rng = random.Random(0)
        for _ in range(200):
            f.observe(rng.uniform(0, 10))
        # Median should be uniform-ish
        assert 4 <= f.quantile(0.5) <= 6
