"""Lineage queries.

Given a How-provenance polynomial annotation, recover:
  - lineage:        set of all input tokens that contributed
  - witnesses:      number of distinct derivations
  - multiplicity:   total count (sum of coefficients)
  - exact_probability:  for probabilistic databases under arbitrary
                         correlation, inclusion-exclusion on the polynomial
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from prov.semiring.how import Polynomial


def lineage(poly: Polynomial) -> set:
    """All input tokens appearing in `poly`."""
    return poly.variables


def witnesses(poly: Polynomial) -> int:
    """Number of distinct monomials with nonzero coefficient."""
    return sum(1 for c in poly.coeffs.values() if c != 0)


def multiplicity(poly: Polynomial) -> int:
    """Total multiplicity = sum of coefficients."""
    return sum(c for c in poly.coeffs.values() if c != 0)


def exact_probability(poly: Polynomial, probs: dict) -> float:
    """Compute exact P(any monomial holds) under arbitrary correlation.

    Each monomial `x^a * y^b * ...` corresponds to the event "x ∧ y ∧ ...".
    The polynomial's existence corresponds to the union of these events.
    For UCQ (union of conjunctive queries) safe-plan computation this is
    polynomial; for general queries it's #P-hard.

    We use truth-table enumeration over the variables — exact, but
    exponential. Use only for small variable sets.
    """
    variables = sorted(poly.variables, key=repr)
    if len(variables) > 18:
        raise ValueError(
            f"too many variables ({len(variables)}) for truth-table evaluation"
        )
    p_total = 0.0
    n = len(variables)
    for assign in range(1 << n):
        env: dict = {}
        prob: float = 1.0
        for i, v in enumerate(variables):
            on = bool((assign >> i) & 1)
            env[v] = 1 if on else 0
            p = probs.get(v, 0.5)
            prob *= p if on else (1 - p)
        # Polynomial evaluates to nonzero iff query holds under this assign
        val = sum(
            coeff * _eval_monomial(mono, env)
            for mono, coeff in poly.coeffs.items()
        )
        if val != 0:
            p_total += prob
    return p_total


def _eval_monomial(mono, env: dict) -> int:
    out = 1
    for tok, p in mono.factors:
        out *= env.get(tok, 0) ** p
    return out


__all__ = ["exact_probability", "lineage", "multiplicity", "witnesses"]
