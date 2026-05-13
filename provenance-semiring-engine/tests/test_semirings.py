"""Per-instance semiring behaviour."""

from __future__ import annotations

from prov.semiring.bag import BagSemiring
from prov.semiring.boolean import BooleanSemiring
from prov.semiring.how import HowProvenance, Monomial, Polynomial
from prov.semiring.trics import TriCS
from prov.semiring.why import WhyProvenance


class TestBag:
    def test_zero_one(self):
        K = BagSemiring()
        assert K.zero() == 0
        assert K.one() == 1

    def test_sum_product(self):
        K = BagSemiring()
        assert K.sum([1, 2, 3]) == 6
        assert K.product([2, 3, 4]) == 24


class TestBoolean:
    def test_ops(self):
        K = BooleanSemiring()
        assert K.plus(True, False) is True
        assert K.times(True, False) is False


class TestWhy:
    def test_singleton(self):
        W = WhyProvenance()
        s = W.singleton("t1")
        assert s == frozenset({frozenset({"t1"})})

    def test_union(self):
        W = WhyProvenance()
        a = W.singleton("a")
        b = W.singleton("b")
        u = W.plus(a, b)
        # Two distinct witnesses
        assert len(u) == 2

    def test_intersection_combines_tokens(self):
        W = WhyProvenance()
        a = W.singleton("a")
        b = W.singleton("b")
        prod = W.times(a, b)
        # One witness containing both tokens
        assert prod == frozenset({frozenset({"a", "b"})})


class TestHow:
    def test_variable(self):
        x = Polynomial.variable("x")
        assert x.coeffs == {Monomial.of("x"): 1}

    def test_polynomial_arithmetic(self):
        H = HowProvenance()
        x = H.singleton("x")
        y = H.singleton("y")
        # (x + y) * x = x^2 + x*y
        result = H.times(H.plus(x, y), x)
        expected = H.plus(H.times(x, x), H.times(y, x))
        assert result == expected

    def test_zero_absorbs(self):
        H = HowProvenance()
        x = H.singleton("x")
        assert H.times(H.zero(), x) == H.zero()

    def test_monomial_dedup(self):
        # x * x = x^2
        x = Polynomial.variable("x")
        H = HowProvenance()
        sq = H.times(x, x)
        assert sq.coeffs == {Monomial.of("x", 2): 1}

    def test_coefficient_addition(self):
        # x + x = 2x
        x = Polynomial.variable("x")
        H = HowProvenance()
        two_x = H.plus(x, x)
        assert two_x.coeffs == {Monomial.of("x"): 2}

    def test_drop_zero_coefficient(self):
        """Adding negative-equivalent through repeated ops produces correct
        zero handling (only relevant for non-N semirings, but we still
        ensure dict entries with 0 coefficient are dropped)."""
        H = HowProvenance()
        zero = H.zero()
        x = H.singleton("x")
        assert H.plus(zero, x) == x
        assert H.plus(x, zero) == x

    def test_evaluate(self):
        H = HowProvenance()
        x = H.singleton("x")
        y = H.singleton("y")
        # poly = 2*x + x*y
        poly = H.plus(H.times(Polynomial.constant(2), x), H.times(x, y))
        # Substitute x=3, y=5 → 2*3 + 3*5 = 21
        assert H.evaluate(poly, {"x": 3, "y": 5}) == 21


class TestTriCS:
    def test_independent_or(self):
        T = TriCS()
        assert abs(T.plus(0.5, 0.5) - 0.75) < 1e-9
        assert abs(T.plus(0.0, 0.7) - 0.7) < 1e-9

    def test_times(self):
        T = TriCS()
        assert abs(T.times(0.3, 0.5) - 0.15) < 1e-9
