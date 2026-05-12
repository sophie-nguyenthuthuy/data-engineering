import numpy as np
import pytest

from src import (
    LocalConfig, randomized_response, laplace_noise,
    MixNode, shuffle,
    shuffle_amplification, required_eps0_for_target,
    private_histogram,
)


def test_randomized_response_is_unbiased_after_debiasing():
    """If we apply RR and then debias, the recovered distribution matches truth."""
    rng = np.random.default_rng(0)
    cfg = LocalConfig(eps0=2.0, domain_size=4)
    true_pmf = [0.5, 0.3, 0.15, 0.05]
    values = list(rng.choice(4, size=50_000, p=true_pmf))
    est = private_histogram(values, cfg, rng=rng)
    # Each component within reasonable tolerance
    for t, e in zip(true_pmf, est):
        assert abs(t - e) < 0.03


def test_shuffler_preserves_multiset():
    """Records survive the mix (in some order)."""
    nodes = [MixNode.fresh() for _ in range(3)]
    records = [f"record-{i:02d}".encode() for i in range(20)]
    shuffled = shuffle(records, nodes)
    assert sorted(records) == sorted(shuffled)


def test_shuffler_permutes():
    """The output order differs from input (very high probability)."""
    nodes = [MixNode.fresh() for _ in range(3)]
    records = [f"record-{i:02d}".encode() for i in range(50)]
    shuffled = shuffle(records, nodes)
    # Probability of identical order with random shuffle is 1/50! → 0
    assert shuffled != records


def test_amplification_decreases_with_n():
    """Bigger n → stronger central guarantee (smaller central ε).

    Uses ε₀=2 so the bound is below eps0 even at moderate n (otherwise the
    min(eps0, bound) clamp makes both equal to eps0).
    """
    b1 = shuffle_amplification(eps0=2.0, n=10_000, delta=1e-6)
    b2 = shuffle_amplification(eps0=2.0, n=1_000_000, delta=1e-6)
    assert b2.eps_central < b1.eps_central
    assert b2.amplification > b1.amplification


def test_amplification_caps_at_eps0():
    """For small n, central ε never exceeds local ε₀."""
    b = shuffle_amplification(eps0=1.0, n=2, delta=1e-6)
    assert b.eps_central <= 1.0


def test_inverse_solver():
    """Given target central ε, computed ε₀ produces ≤ ε when fed back through."""
    eps_target = 0.5
    n = 10_000
    eps0 = required_eps0_for_target(eps_target, n, delta=1e-6)
    b = shuffle_amplification(eps0=eps0, n=n, delta=1e-6)
    assert b.eps_central <= eps_target + 0.05


def test_laplace_mechanism_is_unbiased():
    rng = np.random.default_rng(42)
    samples = [laplace_noise(100.0, sensitivity=1.0, eps=1.0, rng=rng) for _ in range(50_000)]
    assert abs(np.mean(samples) - 100.0) < 0.5
