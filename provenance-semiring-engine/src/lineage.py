"""Lineage: given an output annotation in HowProvenance, recover the set of
input tokens that contributed (drill-through)."""
from __future__ import annotations

from .semiring import Polynomial


def lineage(poly: Polynomial) -> set:
    """Return the set of all input tokens appearing in the polynomial.

    For deeper inspection, walk poly.coeffs directly.
    """
    return poly.tokens()


def witness_count(poly: Polynomial) -> int:
    """Count distinct monomials = distinct ways the output was derived."""
    return sum(1 for m, c in poly.coeffs.items() if c != 0)


def multiplicity(poly: Polynomial) -> int:
    """Sum of coefficients = total derivations under bag semantics."""
    # Only defined for constant evaluations; for purely symbolic polynomials,
    # we return the coefficient sum evaluated at all-1.
    return sum(c for c in poly.coeffs.values())


__all__ = ["lineage", "witness_count", "multiplicity"]
