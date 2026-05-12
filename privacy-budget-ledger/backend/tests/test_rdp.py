"""
Tests for Rényi DP accounting.
"""
import math
import pytest
from app.composition.rdp import (
    rdp_gaussian,
    rdp_laplace,
    rdp_to_dp,
    best_rdp_to_dp,
    compose_rdp,
    rdp_curve_for_gaussian,
    rdp_curve_for_laplace,
    projected_dp_epsilon,
    ALPHA_ORDERS,
)


class TestRDPGaussian:
    def test_formula(self):
        # ε(α) = α·Δ²/(2σ²)
        assert rdp_gaussian(1.0, 1.0, 2.0) == pytest.approx(1.0)
        assert rdp_gaussian(1.0, 2.0, 4.0) == pytest.approx(4 * 1 / (2 * 4))

    def test_increases_with_alpha(self):
        prev = rdp_gaussian(1.0, 1.0, 1.01)
        for alpha in [2.0, 4.0, 8.0]:
            val = rdp_gaussian(1.0, 1.0, alpha)
            assert val > prev
            prev = val

    def test_decreases_with_sigma(self):
        for sigma in [0.5, 1.0, 2.0, 4.0]:
            assert rdp_gaussian(1.0, sigma, 2.0) == pytest.approx(2 * 1 / (2 * sigma**2))

    def test_sensitivity_scales_quadratically(self):
        base = rdp_gaussian(1.0, 1.0, 2.0)
        assert rdp_gaussian(2.0, 1.0, 2.0) == pytest.approx(4 * base)


class TestRDPLaplace:
    def test_positive(self):
        for alpha in [1.5, 2.0, 4.0]:
            assert rdp_laplace(1.0, 1.0, alpha) > 0

    def test_alpha_1_limit(self):
        # Should return KL-like value (finite positive)
        val = rdp_laplace(1.0, 1.0, 1.0)
        assert val > 0
        assert math.isfinite(val)

    def test_larger_b_smaller_rdp(self):
        # Larger scale b means less privacy loss
        assert rdp_laplace(1.0, 2.0, 2.0) < rdp_laplace(1.0, 1.0, 2.0)

    def test_inf_alpha(self):
        val = rdp_laplace(1.0, 1.0, math.inf)
        assert val == pytest.approx(1.0)  # limit = s = Δ/b = 1


class TestRDPToDP:
    def test_basic_conversion(self):
        eps_dp = rdp_to_dp(1.0, 2.0, 1e-5)
        assert eps_dp > 0
        assert math.isfinite(eps_dp)

    def test_larger_alpha_can_be_tighter(self):
        # For Gaussian with Δ=1, σ=2: different α give different (ε,δ)-DP bounds.
        # The best_rdp_to_dp optimiser picks the tightest α.
        vals = [rdp_to_dp(rdp_gaussian(1.0, 2.0, a), a, 1e-5) for a in [2.0, 4.0, 8.0]]
        # All should be finite and positive
        assert all(math.isfinite(v) and v > 0 for v in vals)
        # Larger α is not always tighter — but the optimal α should give a reasonable bound
        best = min(vals)
        basic_eps = 1.0 * math.sqrt(2 * math.log(1.25 / 1e-5)) / 2.0  # classic Gaussian calibration
        assert best < basic_eps * 1.5  # RDP bound within 50% of classic calibration

    def test_best_rdp_to_dp_optimises_over_alpha(self):
        curve = rdp_curve_for_gaussian(1.0, 1.0)
        best = best_rdp_to_dp(curve, 1e-5)
        # should be finite and reasonable
        assert 0 < best < 10


class TestComposition:
    def test_compose_zero_curves(self):
        composed = compose_rdp([])
        assert all(e == 0.0 for _, e in composed)

    def test_compose_additive(self):
        curve = rdp_curve_for_gaussian(1.0, 1.0)
        composed = compose_rdp([curve, curve])
        for (a1, e1), (a2, e2) in zip(curve, composed):
            assert a1 == a2
            assert e2 == pytest.approx(2 * e1)

    def test_composition_tighter_than_basic(self):
        # 10 Gaussian queries at σ=1 should give RDP-composed ε < 10 × single-query ε
        curve = rdp_curve_for_gaussian(1.0, 1.0)
        single_dp = best_rdp_to_dp(curve, 1e-5)
        composed = compose_rdp([curve] * 10)
        composed_dp = best_rdp_to_dp(composed, 1e-5)
        # RDP composition: 10× worse RDP → should be < 10× basic
        assert composed_dp < 10 * single_dp

    def test_projected_epsilon(self):
        base = [(a, 0.0) for a in ALPHA_ORDERS]
        new_curve = rdp_curve_for_gaussian(1.0, 1.0)
        proj = projected_dp_epsilon(base, new_curve, 1e-5)
        single = best_rdp_to_dp(new_curve, 1e-5)
        assert proj == pytest.approx(single, rel=1e-4)
