"""Semiring axiom tests across all instances.

Hypothesis generates random triples (a, b, c) and checks:
  - plus identity:    a ⊕ 0 = a
  - plus commutative: a ⊕ b = b ⊕ a
  - plus associative: (a ⊕ b) ⊕ c = a ⊕ (b ⊕ c)
  - times identity:   a ⊗ 1 = a
  - times commutative: a ⊗ b = b ⊗ a
  - times associative: (a ⊗ b) ⊗ c = a ⊗ (b ⊗ c)
  - distributivity:   a ⊗ (b ⊕ c) = (a ⊗ b) ⊕ (a ⊗ c)
  - absorbing zero:   0 ⊗ a = 0
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from prov.semiring.bag import BagSemiring
from prov.semiring.boolean import BooleanSemiring
from prov.semiring.how import HowProvenance
from prov.semiring.trics import TriCS
from prov.semiring.why import WhyProvenance


def _check(K, a, b, c):
    assert K.plus(a, K.zero()) == a
    assert K.plus(a, b) == K.plus(b, a)
    assert K.plus(K.plus(a, b), c) == K.plus(a, K.plus(b, c))
    assert K.times(a, K.one()) == a
    assert K.times(a, b) == K.times(b, a)
    assert K.times(K.times(a, b), c) == K.times(a, K.times(b, c))
    assert K.times(a, K.plus(b, c)) == K.plus(K.times(a, b), K.times(a, c))
    assert K.times(K.zero(), a) == K.zero()


@given(a=st.integers(0, 100), b=st.integers(0, 100), c=st.integers(0, 100))
@settings(max_examples=60)
@pytest.mark.property
def test_bag_axioms(a: int, b: int, c: int):
    _check(BagSemiring(), a, b, c)


@given(a=st.booleans(), b=st.booleans(), c=st.booleans())
@settings(max_examples=20)
def test_boolean_axioms(a: bool, b: bool, c: bool):
    _check(BooleanSemiring(), a, b, c)


def test_why_axioms_concrete():
    W = WhyProvenance()
    a = W.singleton("x")
    b = W.singleton("y")
    c = W.singleton("z")
    _check(W, a, b, c)


def test_how_axioms_concrete():
    H = HowProvenance()
    a = H.singleton("x")
    b = H.singleton("y")
    c = H.singleton("z")
    _check(H, a, b, c)


def test_how_distributivity_polynomial():
    """Specifically verify a ⊗ (b ⊕ c) = a*b + a*c, the heart of provenance."""
    H = HowProvenance()
    a = H.singleton("a")
    b = H.singleton("b")
    c = H.singleton("c")
    lhs = H.times(a, H.plus(b, c))
    rhs = H.plus(H.times(a, b), H.times(a, c))
    assert lhs == rhs


@given(
    a=st.floats(0.0, 1.0, allow_nan=False),
    b=st.floats(0.0, 1.0, allow_nan=False),
    c=st.floats(0.0, 1.0, allow_nan=False),
)
@settings(max_examples=60)
@pytest.mark.property
def test_trics_monoid_axioms_within_tolerance(a: float, b: float, c: float):
    """TriCS satisfies the two monoid axioms but NOT distributivity in
    general (it's only a true semiring for independent events). We check
    everything except distributivity."""
    T = TriCS()
    def approx_eq(x, y):
        return abs(x - y) < 1e-9
    assert approx_eq(T.plus(a, T.zero()), a)
    assert approx_eq(T.plus(a, b), T.plus(b, a))
    assert approx_eq(T.plus(T.plus(a, b), c), T.plus(a, T.plus(b, c)))
    assert approx_eq(T.times(a, T.one()), a)
    assert approx_eq(T.times(a, b), T.times(b, a))
    assert approx_eq(T.times(T.times(a, b), c), T.times(a, T.times(b, c)))


def test_trics_distributivity_known_failure():
    """Document that TriCS is not a true semiring: distributivity fails."""
    T = TriCS()
    # a ⊗ (b ⊕ c) = 0.5 * 1 = 0.5
    # (a⊗b) ⊕ (a⊗c) = 0.5 ⊕ 0.5 = 1 - 0.25 = 0.75
    a, b, c = 0.5, 1.0, 1.0
    lhs = T.times(a, T.plus(b, c))
    rhs = T.plus(T.times(a, b), T.times(a, c))
    assert lhs != rhs   # the failure is documented behaviour
