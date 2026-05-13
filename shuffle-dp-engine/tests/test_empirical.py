"""Empirical DP validation."""

from __future__ import annotations

import pytest

from sdp.empirical import empirical_advantage_rr


@pytest.mark.empirical
def test_advantage_within_theoretical_bound():
    """For ε₀ ∈ {1, 2, 4}, empirical advantage should be ≤ theoretical."""
    for eps in (1.0, 2.0, 4.0):
        r = empirical_advantage_rr(eps0=eps, domain_size=4, n_trials=30_000)
        # Allow small slack from sampling noise
        assert r.advantage <= r.theoretical_bound + 0.02


def test_advantage_grows_with_epsilon():
    r1 = empirical_advantage_rr(eps0=0.5, domain_size=4, n_trials=20_000)
    r2 = empirical_advantage_rr(eps0=4.0, domain_size=4, n_trials=20_000)
    assert r2.advantage > r1.advantage


def test_advantage_zero_at_very_low_eps():
    """At ε₀ = 0.01 the mechanism is nearly random; advantage should be tiny."""
    r = empirical_advantage_rr(eps0=0.01, domain_size=4, n_trials=20_000)
    assert r.advantage < 0.05
