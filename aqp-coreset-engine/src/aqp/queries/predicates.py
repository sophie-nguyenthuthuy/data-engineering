"""Predicate language for coreset queries.

A predicate is a ``Callable[[tuple[float, ...]], bool]`` that decides
whether a row (its payload tuple) is selected by a query. We provide a
small combinator library on top so users can write queries declaratively
and so the empirical-validation harness can sample queries from a fixed
class with bounded VC-dimension.

The combinators are deliberately tiny and stateless — no DSL parser, no
expression tree. Composition is via plain functions.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeAlias

Payload: TypeAlias = tuple[float, ...]
Predicate: TypeAlias = Callable[[Payload], bool]


def always_true() -> Predicate:
    """Predicate that selects every row — the unconstrained aggregate."""

    return lambda _payload: True


def eq_pred(col: int, value: float) -> Predicate:
    """``payload[col] == value`` equality predicate."""
    if col < 0:
        raise ValueError("col must be ≥ 0")

    def _eq(payload: Payload) -> bool:
        return payload[col] == value

    return _eq


def range_pred(col: int, lo: float, hi: float) -> Predicate:
    """Closed-range predicate ``lo ≤ payload[col] ≤ hi``."""
    if col < 0:
        raise ValueError("col must be ≥ 0")
    if hi < lo:
        raise ValueError("hi must be ≥ lo")

    def _range(payload: Payload) -> bool:
        return lo <= payload[col] <= hi

    return _range


def box_pred(bounds: dict[int, tuple[float, float]]) -> Predicate:
    """Axis-aligned box: every listed column must lie in its [lo, hi]."""
    if not bounds:
        raise ValueError("bounds must be non-empty")
    for col, (lo, hi) in bounds.items():
        if col < 0:
            raise ValueError("col must be ≥ 0")
        if hi < lo:
            raise ValueError("hi must be ≥ lo")
    items = tuple(bounds.items())

    def _box(payload: Payload) -> bool:
        for col, (lo, hi) in items:
            v = payload[col]
            if not (lo <= v <= hi):
                return False
        return True

    return _box


def and_(*preds: Predicate) -> Predicate:
    """Logical AND of zero or more predicates (empty AND ≡ True)."""
    if not preds:
        return always_true()
    fns = tuple(preds)

    def _and(payload: Payload) -> bool:
        return all(p(payload) for p in fns)

    return _and


__all__ = [
    "Payload",
    "Predicate",
    "always_true",
    "and_",
    "box_pred",
    "eq_pred",
    "range_pred",
]
