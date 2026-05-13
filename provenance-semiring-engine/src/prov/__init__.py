"""Provenance Semiring Engine.

Semiring-based data provenance (Green-Karvounarakis-Tannen, PODS 2007).
"""

from __future__ import annotations

__version__ = "0.1.0"

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from prov.operators import aggregate, annotate, join, project, select, union
    from prov.semiring.bag import BagSemiring
    from prov.semiring.boolean import BooleanSemiring
    from prov.semiring.how import HowProvenance, Monomial, Polynomial
    from prov.semiring.trics import TriCS
    from prov.semiring.why import WhyProvenance

_LAZY: dict[str, str] = {
    "BagSemiring": "prov.semiring.bag",
    "BooleanSemiring": "prov.semiring.boolean",
    "WhyProvenance": "prov.semiring.why",
    "HowProvenance": "prov.semiring.how",
    "Polynomial": "prov.semiring.how",
    "Monomial": "prov.semiring.how",
    "TriCS": "prov.semiring.trics",
    "annotate": "prov.operators",
    "project": "prov.operators",
    "select": "prov.operators",
    "union": "prov.operators",
    "join": "prov.operators",
    "aggregate": "prov.operators",
}


def __getattr__(name: str) -> Any:
    mod = _LAZY.get(name)
    if mod is None:
        raise AttributeError(f"module 'prov' has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(mod), name)


__all__ = [
    "BagSemiring",
    "BooleanSemiring",
    "HowProvenance",
    "Monomial",
    "Polynomial",
    "TriCS",
    "WhyProvenance",
    "aggregate",
    "annotate",
    "join",
    "project",
    "select",
    "union",
]
