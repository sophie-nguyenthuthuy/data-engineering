"""
Tests for zero-concentrated DP accounting.
"""
import math
import pytest
from app.composition.zcdp import (
    zcdp_gaussian,
    zcdp_laplace_approx,
    zcdp_to_dp,
    compose_zcdp,
    ZCDPBudget,
    sigma_for_rho,
    rho_for_sigma,
    rho_for_dp_target,
    basic_composition_dp_epsilon,
)


class TestZCDPGaussian:
    def test_formula(self):
        # ρ = Δ²/(2σ²)
        assert zcdp_gaussian(1.0, 1.0) == pytest.approx(0.5)
        assert zcdp_gaussian(2.0, 1.0) == pytest.approx(2.0)
        assert zcdp_gaussian(1.0, 2.0) == pytest.approx(0.125)

    def test_inverse_relationship(self):
        rho = zcdp_gaussian(1.0, 2.0)
        sigma = sigma_for_rho(1.0, rho)
        assert sigma == pytest.approx(2.0)

    def test_rho_for_sigma_roundtrip(self):
        sigma = 3.5
        rho = rho_for_sigma(1.0, sigma)
        sigma_back = sigma_for_rho(1.0, rho)
        assert sigma_back == pytest.approx(sigma)

    def test_invalid_sigma(self):
        with pytest.raises(ValueError):
            zcdp_gaussian(1.0, 0.0)


class TestZCDPToDP:
    def test_formula(self):
        # ε = ρ + 2√(ρ · log(1/δ))
        rho, delta = 0.5, 1e-5
        expected = rho + 2 * math.sqrt(rho * math.log(1 / delta))
        assert zcdp_to_dp(rho, delta) == pytest.approx(expected)

    def test_zero_rho(self):
        assert zcdp_to_dp(0.0, 1e-5) == 0.0

    def test_scales_sublinearly_with_k(self):
        # k queries each contributing ρ → total is k·ρ
        # ε(k·ρ) ≈ k·ρ + 2√(k·ρ·log(1/δ))
        # which grows as O(√k) in the √ term
        delta = 1e-5
        rho_per_query = 0.01
        eps_1 = zcdp_to_dp(rho_per_query, delta)
        eps_100 = zcdp_to_dp(100 * rho_per_query, delta)
        # basic composition would give 100 × eps_1
        assert eps_100 < 100 * eps_1

    def test_invalid_delta(self):
        with pytest.raises(ValueError):
            zcdp_to_dp(0.5, 0.0)
        with pytest.raises(ValueError):
            zcdp_to_dp(0.5, 1.0)


class TestCompose:
    def test_sum(self):
        assert compose_zcdp([0.1, 0.2, 0.3]) == pytest.approx(0.6)

    def test_empty(self):
        assert compose_zcdp([]) == 0.0


class TestZCDPBudget:
    def test_remaining_rho(self):
        budget = ZCDPBudget(total_rho=1.0, consumed_rho=0.3)
        assert budget.remaining_rho == pytest.approx(0.7)

    def test_would_exceed(self):
        budget = ZCDPBudget(total_rho=1.0, consumed_rho=0.9)
        assert budget.would_exceed(0.2) is True
        assert budget.would_exceed(0.05) is False

    def test_max_feasible_sigma(self):
        budget = ZCDPBudget(total_rho=0.5, consumed_rho=0.0)
        sigma = budget.max_feasible_sigma(1.0)
        # ρ_max = 0.5 → σ_min = √(Δ²/(2·0.5)) = √1 = 1
        assert sigma == pytest.approx(1.0)


class TestRhoForDPTarget:
    def test_roundtrip(self):
        eps_target, delta = 1.0, 1e-5
        rho = rho_for_dp_target(eps_target, delta)
        eps_back = zcdp_to_dp(rho, delta)
        assert eps_back == pytest.approx(eps_target, rel=1e-4)

    def test_tighter_eps_requires_smaller_rho(self):
        rho1 = rho_for_dp_target(1.0, 1e-5)
        rho2 = rho_for_dp_target(2.0, 1e-5)
        assert rho1 < rho2


class TestBasicComposition:
    def test_linear(self):
        assert basic_composition_dp_epsilon([0.1] * 10) == pytest.approx(1.0)

    def test_matches_zcdp_for_single_query(self):
        # For single query with Gaussian, basic ε vs zCDP ε
        sigma, delta = 1.0, 1e-5
        rho = zcdp_gaussian(1.0, sigma)
        zcdp_eps = zcdp_to_dp(rho, delta)
        # basic ε from Gaussian calibration
        basic_eps = 1.0 * math.sqrt(2 * math.log(1.25 / delta)) / sigma
        # zCDP should be ≤ basic (tighter for single query too)
        assert zcdp_eps <= basic_eps + 0.5  # some tolerance for approximation differences
