"""Commutative semirings (K, ⊕, ⊗, 0, 1) for provenance annotation.

Three instances:
  - Bag       (N, +, *, 0, 1)              — multiset counting
  - WhyProv   (2^X, ∪, ×, ∅, {()})         — sets of witness combinations
  - HowProv   (N[X], +, *, 0, 1)           — polynomials over input tokens
  - TriCS     ([0,1], ⊕ₚ, *, 0, 1)         — probabilistic (independent-OR)

Every semiring K supports zero(), one(), plus(a,b), times(a,b). Operators read
generically — see operators.py.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Hashable, TypeVar

T = TypeVar("T")


class Semiring(ABC, Generic[T]):
    @abstractmethod
    def zero(self) -> T: ...
    @abstractmethod
    def one(self) -> T: ...
    @abstractmethod
    def plus(self, a: T, b: T) -> T: ...
    @abstractmethod
    def times(self, a: T, b: T) -> T: ...

    # Convenience -----------------------------------------------------------
    def sum(self, xs):
        acc = self.zero()
        for x in xs:
            acc = self.plus(acc, x)
        return acc

    def product(self, xs):
        acc = self.one()
        for x in xs:
            acc = self.times(acc, x)
        return acc


# ---------------------------------------------------------------------------
# Bag (counting)
# ---------------------------------------------------------------------------

class BagSemiring(Semiring[int]):
    def zero(self) -> int: return 0
    def one(self) -> int: return 1
    def plus(self, a: int, b: int) -> int: return a + b
    def times(self, a: int, b: int) -> int: return a * b


# ---------------------------------------------------------------------------
# Why-provenance: set of witness combinations.
#   Element type: frozenset[ frozenset[token] ]
#   Each inner frozenset is one witness (a join combination); the outer set is
#   the alternatives (unions).
# ---------------------------------------------------------------------------

Witness = frozenset  # frozenset[Hashable]


class WhyProvenance(Semiring[frozenset]):
    """Token = input row id (any hashable)."""

    def zero(self) -> frozenset: return frozenset()
    def one(self) -> frozenset: return frozenset({frozenset()})

    def plus(self, a: frozenset, b: frozenset) -> frozenset:
        return a | b

    def times(self, a: frozenset, b: frozenset) -> frozenset:
        return frozenset(w1 | w2 for w1 in a for w2 in b)

    @staticmethod
    def singleton(token: Hashable) -> frozenset:
        return frozenset({frozenset({token})})


# ---------------------------------------------------------------------------
# How-provenance: polynomials in N[X].
#   Represented as a dict from monomial → coefficient.
#   Monomial = frozen multiset of tokens, as tuple of sorted (token, power).
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Monomial:
    """Sorted tuple of (token, power)."""
    factors: tuple  # tuple[(token, int), ...]

    @staticmethod
    def empty() -> "Monomial":
        return Monomial(())

    @staticmethod
    def of(token: Hashable, power: int = 1) -> "Monomial":
        return Monomial(((token, power),))

    def times(self, other: "Monomial") -> "Monomial":
        merged: dict = {}
        for tok, p in self.factors + other.factors:
            merged[tok] = merged.get(tok, 0) + p
        items = tuple(sorted(merged.items()))
        return Monomial(items)

    def __repr__(self) -> str:
        if not self.factors:
            return "1"
        return "*".join(f"{t}^{p}" if p != 1 else f"{t}" for t, p in self.factors)


@dataclass(frozen=True)
class Polynomial:
    """coeffs: dict mapping Monomial → int (sum of coefficients)."""
    coeffs: dict  # dict[Monomial, int]

    def __repr__(self) -> str:
        if not self.coeffs:
            return "0"
        terms = []
        for mono, c in sorted(self.coeffs.items(), key=lambda x: repr(x[0])):
            if c == 0:
                continue
            if mono == Monomial.empty():
                terms.append(str(c))
            elif c == 1:
                terms.append(repr(mono))
            else:
                terms.append(f"{c}*{repr(mono)}")
        return " + ".join(terms) if terms else "0"

    def tokens(self) -> set:
        out = set()
        for mono in self.coeffs:
            for tok, _ in mono.factors:
                out.add(tok)
        return out


class HowProvenance(Semiring[Polynomial]):
    def zero(self) -> Polynomial: return Polynomial({})
    def one(self) -> Polynomial: return Polynomial({Monomial.empty(): 1})

    def plus(self, a: Polynomial, b: Polynomial) -> Polynomial:
        out = dict(a.coeffs)
        for m, c in b.coeffs.items():
            out[m] = out.get(m, 0) + c
        # Drop zero coeffs
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
        return Polynomial({Monomial.of(token): 1})


# ---------------------------------------------------------------------------
# TriCS: probabilistic semiring with independent-OR plus.
#   p ⊕ q = 1 - (1-p)(1-q)
#   p ⊗ q = p * q
# Note: This is an *approximation* of the lifted probabilistic semantics
# (assumes independence). For exact, you'd evaluate the How-polynomial.
# ---------------------------------------------------------------------------

class TriCS(Semiring[float]):
    def zero(self) -> float: return 0.0
    def one(self) -> float: return 1.0

    def plus(self, a: float, b: float) -> float:
        return 1.0 - (1.0 - a) * (1.0 - b)

    def times(self, a: float, b: float) -> float:
        return a * b


__all__ = [
    "Semiring", "BagSemiring",
    "WhyProvenance", "Witness",
    "HowProvenance", "Polynomial", "Monomial",
    "TriCS",
]
