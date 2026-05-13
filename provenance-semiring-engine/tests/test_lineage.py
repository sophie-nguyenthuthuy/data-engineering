"""Lineage queries."""

from __future__ import annotations

from prov.lineage import exact_probability, lineage, multiplicity, witnesses
from prov.semiring.how import HowProvenance


def test_lineage_extracts_tokens():
    H = HowProvenance()
    x = H.singleton("x")
    y = H.singleton("y")
    poly = H.plus(H.times(x, y), x)
    assert lineage(poly) == {"x", "y"}


def test_witnesses_counts_monomials():
    H = HowProvenance()
    x = H.singleton("x")
    y = H.singleton("y")
    z = H.singleton("z")
    poly = H.plus(H.plus(H.times(x, y), x), z)
    # Monomials: x*y, x, z → 3 witnesses
    assert witnesses(poly) == 3


def test_multiplicity_is_sum_of_coefficients():
    H = HowProvenance()
    x = H.singleton("x")
    # 3 * x = x + x + x → coefficient 3
    poly = H.plus(H.plus(x, x), x)
    assert multiplicity(poly) == 3


def test_exact_probability_single_var():
    H = HowProvenance()
    x = H.singleton("x")
    # Just x → probability = P(x)
    p = exact_probability(x, {"x": 0.7})
    assert abs(p - 0.7) < 1e-9


def test_exact_probability_independent_or():
    H = HowProvenance()
    x = H.singleton("x")
    y = H.singleton("y")
    poly = H.plus(x, y)
    # P(x ∨ y) = 1 - (1-px)(1-py) = 1 - 0.5 * 0.3 = 0.85
    p = exact_probability(poly, {"x": 0.5, "y": 0.7})
    assert abs(p - 0.85) < 1e-9


def test_exact_probability_correlated():
    """For x*y (P(x ∧ y)) and x + x*y (which is x), exact probability handles
    the correlation correctly (unlike naive multiplication)."""
    H = HowProvenance()
    x = H.singleton("x")
    y = H.singleton("y")
    # x + x*y simplifies to x*(1+y), but in N[X] x + x*y is just x*(1+y)
    poly = H.plus(x, H.times(x, y))
    # Truth-table over {x, y}:
    #   x=0, y=0: poly evals to 0 → no contribution
    #   x=0, y=1: poly evals to 0 → no contribution
    #   x=1, y=0: poly evals to 1 → contribute P(x=1)*P(y=0)
    #   x=1, y=1: poly evals to 2 → contribute P(x=1)*P(y=1)
    # → P = P(x=1) (regardless of y)
    p = exact_probability(poly, {"x": 0.3, "y": 0.5})
    assert abs(p - 0.3) < 1e-9


def test_exact_probability_too_many_variables_raises():
    H = HowProvenance()
    poly = H.zero()
    for i in range(20):
        poly = H.plus(poly, H.singleton(f"v{i}"))
    import pytest
    with pytest.raises(ValueError, match="too many variables"):
        exact_probability(poly, {})
