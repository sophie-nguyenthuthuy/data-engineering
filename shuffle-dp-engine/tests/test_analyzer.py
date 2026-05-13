"""Balle analyzer + composition."""

from __future__ import annotations

import pytest

from sdp.analyzer.balle import required_eps0_for_target, shuffle_amplification
from sdp.analyzer.composition import composed_bound


def test_amplification_decreases_with_n():
    b1 = shuffle_amplification(eps0=2.0, n=10_000, delta=1e-6)
    b2 = shuffle_amplification(eps0=2.0, n=1_000_000, delta=1e-6)
    assert b2.eps_central < b1.eps_central
    assert b2.amplification > b1.amplification


def test_amplification_caps_at_eps0():
    b = shuffle_amplification(eps0=1.0, n=2, delta=1e-6)
    assert b.eps_central <= 1.0


def test_amplification_validates_inputs():
    with pytest.raises(ValueError):
        shuffle_amplification(eps0=0, n=1000)
    with pytest.raises(ValueError):
        shuffle_amplification(eps0=1, n=0)
    with pytest.raises(ValueError):
        shuffle_amplification(eps0=1, n=1000, delta=2)


def test_inverse_solver_round_trips():
    eps_target = 0.5
    n = 10_000
    eps0 = required_eps0_for_target(eps_target, n, delta=1e-6)
    b = shuffle_amplification(eps0=eps0, n=n, delta=1e-6)
    assert b.eps_central <= eps_target + 0.05


def test_basic_composition_sums():
    b1 = shuffle_amplification(eps0=2.0, n=10_000)
    b2 = shuffle_amplification(eps0=2.0, n=10_000)
    c = composed_bound([b1, b2], method="basic")
    assert c.eps_total == b1.eps_central + b2.eps_central


def test_advanced_composition_tighter_than_basic_at_scale():
    b = shuffle_amplification(eps0=1.0, n=100_000)
    bounds = [b] * 100
    basic = composed_bound(bounds, method="basic")
    adv = composed_bound(bounds, method="advanced", target_delta=1e-5)
    # Advanced should be tighter for k=100 mechanisms
    assert adv.eps_total < basic.eps_total


def test_empty_composition():
    c = composed_bound([], method="basic")
    assert c.eps_total == 0.0
    assert c.n_mechanisms == 0


def test_advanced_requires_target_delta():
    b = shuffle_amplification(eps0=1.0, n=10_000)
    with pytest.raises(ValueError):
        composed_bound([b], method="advanced", target_delta=0)


def test_unknown_method_raises():
    with pytest.raises(ValueError):
        composed_bound([], method="snake-oil")
