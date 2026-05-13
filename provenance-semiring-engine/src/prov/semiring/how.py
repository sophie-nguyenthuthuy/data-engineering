"""How-provenance: polynomials in N[X].

Each annotation is a polynomial in the semiring `N[X]` of integer
coefficients over variables (input tokens). This is the **most informative**
finite-domain semiring — it tracks not just *which* inputs contributed
but *how* (with what multiplicities, in which combinations).

Sparse representation: `Polynomial.coeffs: dict[Monomial, int]`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from prov.semiring.base import Semiring

if TYPE_CHECKING:
    from collections.abc import Hashable


@dataclass(frozen=True, slots=True)
class Monomial:
    """Product of variable powers, e.g. x^2 * y * z^3.

    Internal representation: sorted tuple of (token, power) pairs.
    The empty tuple is the unit monomial (= 1).
    """

    factors: tuple

    @staticmethod
    def empty() -> Monomial:
        return Monomial(())

    @staticmethod
    def of(token: Hashable, power: int = 1) -> Monomial:
        if power < 0:
            raise ValueError("monomial power must be ≥ 0")
        if power == 0:
            return Monomial.empty()
        return Monomial(((token, power),))

    def times(self, other: Monomial) -> Monomial:
        merged: dict = {}
        for tok, p in self.factors + other.factors:
            merged[tok] = merged.get(tok, 0) + p
        items = tuple(sorted(merged.items(), key=lambda kv: repr(kv[0])))
        return Monomial(items)

    @property
    def variables(self) -> set:
        return {tok for tok, _p in self.factors}

    def __repr__(self) -> str:
        if not self.factors:
            return "1"
        parts = []
        for tok, p in self.factors:
            parts.append(f"{tok}^{p}" if p != 1 else str(tok))
        return "*".join(parts)


@dataclass(frozen=True)
class Polynomial:
    """Sparse polynomial in N[X]."""

    coeffs: dict   # Monomial -> int

    @staticmethod
    def zero() -> Polynomial:
        return Polynomial({})

    @staticmethod
    def one() -> Polynomial:
        return Polynomial({Monomial.empty(): 1})

    @staticmethod
    def variable(token: Hashable) -> Polynomial:
        return Polynomial({Monomial.of(token): 1})

    @staticmethod
    def constant(c: int) -> Polynomial:
        if c == 0:
            return Polynomial.zero()
        return Polynomial({Monomial.empty(): c})

    @property
    def variables(self) -> set:
        out: set = set()
        for mono in self.coeffs:
            out |= mono.variables
        return out

    @property
    def is_zero(self) -> bool:
        return not self.coeffs

    def __repr__(self) -> str:
        if not self.coeffs:
            return "0"
        terms = []
        for mono, c in sorted(self.coeffs.items(), key=lambda kv: repr(kv[0])):
            if c == 0:
                continue
            if mono == Monomial.empty():
                terms.append(str(c))
            elif c == 1:
                terms.append(repr(mono))
            else:
                terms.append(f"{c}*{mono!r}")
        return " + ".join(terms) if terms else "0"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Polynomial):
            return NotImplemented
        # Drop zero coefficients before comparing
        lhs = {m: c for m, c in self.coeffs.items() if c != 0}
        rhs = {m: c for m, c in other.coeffs.items() if c != 0}
        return lhs == rhs

    def __hash__(self) -> int:
        return hash(tuple(sorted(
            ((m, c) for m, c in self.coeffs.items() if c != 0),
            key=lambda kv: repr(kv[0]),
        )))


class HowProvenance(Semiring[Polynomial]):
    """N[X] semiring."""

    def zero(self) -> Polynomial:
        return Polynomial.zero()

    def one(self) -> Polynomial:
        return Polynomial.one()

    def plus(self, a: Polynomial, b: Polynomial) -> Polynomial:
        out = dict(a.coeffs)
        for m, c in b.coeffs.items():
            out[m] = out.get(m, 0) + c
        out = {m: c for m, c in out.items() if c != 0}
        return Polynomial(out)

    def times(self, a: Polynomial, b: Polynomial) -> Polynomial:
        out: dict = {}
        for ma, ca in a.coeffs.items():
            for mb, cb in b.coeffs.items():
                m = ma.times(mb)
                out[m] = out.get(m, 0) + ca * cb
        out = {m: c for m, c in out.items() if c != 0}
        return Polynomial(out)

    @staticmethod
    def singleton(token: Hashable) -> Polynomial:
        return Polynomial.variable(token)

    # ---- Evaluation under variable substitution --------------------------

    @staticmethod
    def evaluate(poly: Polynomial, env: dict) -> object:
        """Substitute variable → value, return the resulting number.

        Treats unknown variables as 0.
        """
        total: float = 0
        for mono, coeff in poly.coeffs.items():
            term: float = coeff
            for tok, p in mono.factors:
                if tok not in env:
                    term = 0
                    break
                term *= env[tok] ** p
            total += term
        return total


__all__ = ["HowProvenance", "Monomial", "Polynomial"]
