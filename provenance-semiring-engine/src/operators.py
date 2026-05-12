"""Annotated relational algebra operators.

A relation is a dict mapping *tuple key* (hashable) → annotation in K.
The same operator code works for every semiring.
"""
from __future__ import annotations

from typing import Callable, Hashable

from .semiring import Semiring


# Tuples are tuples of values; we use them directly as dict keys.
Relation = dict  # dict[tuple, K-annotation]


def project(rel: Relation, indices: tuple, K: Semiring) -> Relation:
    """π_{indices}(R) — project to a sub-tuple. Annotations collapse via ⊕."""
    out: Relation = {}
    for tup, ann in rel.items():
        key = tuple(tup[i] for i in indices)
        if key in out:
            out[key] = K.plus(out[key], ann)
        else:
            out[key] = ann
    return out


def select(rel: Relation, predicate: Callable, K: Semiring) -> Relation:
    """σ_{predicate}(R) — annotations unchanged."""
    return {tup: ann for tup, ann in rel.items() if predicate(tup)}


def union(a: Relation, b: Relation, K: Semiring) -> Relation:
    """R ∪ S — annotations combine via ⊕."""
    out = dict(a)
    for tup, ann in b.items():
        out[tup] = K.plus(out[tup], ann) if tup in out else ann
    return out


def join(a: Relation, b: Relation, key_a: tuple, key_b: tuple, K: Semiring) -> Relation:
    """R ⋈ S — natural-style join on keyed columns; annotations multiply."""
    # Hash-build on b
    index: dict = {}
    for tup_b, ann_b in b.items():
        k = tuple(tup_b[i] for i in key_b)
        index.setdefault(k, []).append((tup_b, ann_b))
    out: Relation = {}
    for tup_a, ann_a in a.items():
        k = tuple(tup_a[i] for i in key_a)
        for tup_b, ann_b in index.get(k, ()):
            joined = tup_a + tup_b
            ann = K.times(ann_a, ann_b)
            out[joined] = K.plus(out[joined], ann) if joined in out else ann
    return out


def aggregate(rel: Relation, group_indices: tuple, K: Semiring) -> Relation:
    """γ_{group_indices}(R) — same as projection w.r.t. annotation algebra."""
    return project(rel, group_indices, K)


# ---------------------------------------------------------------------------
# Annotation construction helpers
# ---------------------------------------------------------------------------

def annotate(tuples: list, token_fn: Callable, K: Semiring) -> Relation:
    """Build a Relation from python tuples; each gets an annotation produced
    by token_fn(idx, tuple)."""
    out: Relation = {}
    for i, t in enumerate(tuples):
        ann = token_fn(i, t)
        out[t] = K.plus(out[t], ann) if t in out else ann
    return out


__all__ = ["Relation", "project", "select", "union", "join", "aggregate", "annotate"]
