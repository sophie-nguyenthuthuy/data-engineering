"""Annotated relational-algebra operators.

A Relation is `dict[tuple, K-annotation]`. Every operator is parameterised
by the semiring K and handles annotation algebra uniformly.

Operations:
  annotate   build an annotated relation from raw tuples
  select     σ_p : annotations unchanged for surviving tuples
  project    π_indices : annotations of equal projections combined via ⊕
  union      R ∪ S : combine matching tuples' annotations via ⊕
  join       R ⋈ S : combine annotations via ⊗ for matched tuples
  aggregate  γ_indices : same algebra as project
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from prov.semiring.base import Semiring

Tuple = tuple
Relation = dict   # dict[tuple, K]


def annotate(
    rows: Iterable[Tuple], token_fn: Callable, K: Semiring,
) -> Relation:
    """Build `Relation` from raw rows.

    `token_fn(i, row)` returns the annotation for row #i.
    Duplicate rows combine via K.plus.
    """
    out: Relation = {}
    for i, row in enumerate(rows):
        a = token_fn(i, row)
        out[row] = K.plus(out[row], a) if row in out else a
    return out


def select(rel: Relation, predicate: Callable, K: Semiring) -> Relation:
    """σ_p : keep tuples where predicate(tup) is True; annotations unchanged."""
    return {tup: ann for tup, ann in rel.items() if predicate(tup)}


def project(rel: Relation, indices: tuple, K: Semiring) -> Relation:
    """π_indices : project sub-tuple; collapse equal-projection annotations via ⊕."""
    out: Relation = {}
    for tup, ann in rel.items():
        key = tuple(tup[i] for i in indices)
        out[key] = K.plus(out[key], ann) if key in out else ann
    return out


def union(a: Relation, b: Relation, K: Semiring) -> Relation:
    """R ∪ S : combine matching tuples' annotations via ⊕."""
    out = dict(a)
    for tup, ann in b.items():
        out[tup] = K.plus(out[tup], ann) if tup in out else ann
    return out


def join(
    a: Relation, b: Relation, key_a: tuple, key_b: tuple, K: Semiring,
) -> Relation:
    """R ⋈ S : equality join on the given key columns.

    Output tuples are (tup_a + tup_b); annotations multiply via ⊗.
    Duplicate output tuples (multiple match pairs) sum via ⊕.
    """
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
            new_ann = K.times(ann_a, ann_b)
            out[joined] = K.plus(out[joined], new_ann) if joined in out else new_ann
    return out


def aggregate(rel: Relation, group_indices: tuple, K: Semiring) -> Relation:
    """γ_group : group by indices; annotations sum via ⊕."""
    return project(rel, group_indices, K)


__all__ = ["Relation", "Tuple", "aggregate", "annotate", "join", "project", "select", "union"]
