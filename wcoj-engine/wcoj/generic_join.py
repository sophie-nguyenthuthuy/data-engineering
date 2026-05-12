"""Generic Join — variable-at-a-time worst-case optimal join.

Reference:
    Ngo, H. Q., Porat, E., Ré, C., & Rudra, A. (2012). Skew strikes back:
    New developments in the theory of join algorithms. SIGMOD Record.

Algorithm:
    For each variable x (in order), intersect the domains of x across all
    relations that contain x, then recurse with x bound to each value in the
    intersection.

Complexity: Same asymptotic bound as LFTJ — O(N^{rho*}).  In practice LFTJ
is faster because it avoids materialising intermediate sets, but Generic Join
is simpler to understand and verify.

Public API
----------
generic_join(query, var_order) → np.ndarray
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np

from .query import JoinQuery, Relation


def generic_join(
    query: JoinQuery,
    var_order: Optional[List[str]] = None,
) -> np.ndarray:
    """Run Generic Join on *query* and return result tuples.

    Args:
        query:     The join query.
        var_order: Variable elimination order.  Defaults to query heuristic.

    Returns:
        2-D int64 array of shape (n_results, len(var_order)).
    """
    if var_order is None:
        var_order = query.variable_order()

    results: List[Tuple] = []
    _gj(query.relations, var_order, {}, results)

    k = len(var_order)
    if not results:
        return np.empty((0, k), dtype=np.int64)
    # results are ordered by var_order already
    return np.array(results, dtype=np.int64)


# ------------------------------------------------------------------ #
#  Internal recursive implementation                                  #
# ------------------------------------------------------------------ #

def _gj(
    relations: List[Relation],
    remaining_vars: List[str],
    binding: Dict[str, int],
    output: List[Tuple],
) -> None:
    if not remaining_vars:
        # Emit one result tuple in the original var_order (binding is complete).
        output.append(tuple(binding[v] for v in binding))
        return

    x = remaining_vars[0]
    rest = remaining_vars[1:]

    # Partition relations by whether they mention x.
    x_rels: List[Tuple[Relation, int]] = []   # (relation, column-index-of-x)
    other_rels: List[Relation] = []
    for r in relations:
        if x in r.variables:
            x_rels.append((r, r.variables.index(x)))
        else:
            other_rels.append(r)

    if not x_rels:
        # x is unconstrained — skip (shouldn't happen in a well-formed query).
        _gj(other_rels, rest, binding, output)
        return

    # Compute the intersection of x-domains across all x_rels.
    domain = _sorted_column(x_rels[0][0].data, x_rels[0][1])
    for r, col in x_rels[1:]:
        domain = _intersect_sorted(domain, _sorted_column(r.data, col))
        if len(domain) == 0:
            return

    # For each value v in the domain, substitute x=v and recurse.
    for v in domain:
        # Slice each x_rel to rows where column x == v, drop the x column.
        new_rels: List[Relation] = list(other_rels)
        for r, col in x_rels:
            mask = r.data[:, col] == v
            sub = r.data[mask]
            new_vars = [var for var in r.variables if var != x]
            new_cols = [i for i, var in enumerate(r.variables) if var != x]
            if new_vars:
                new_rels.append(Relation(r.name, new_vars, sub[:, new_cols]))
            # If new_vars is empty the relation is fully satisfied; don't add it.

        new_binding = {**binding, x: v}
        _gj(new_rels, rest, new_binding, output)


# ------------------------------------------------------------------ #
#  Helpers                                                            #
# ------------------------------------------------------------------ #

def _sorted_column(data: np.ndarray, col: int) -> np.ndarray:
    """Return sorted unique values from column *col*."""
    return np.unique(data[:, col])


def _intersect_sorted(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Intersection of two sorted unique int64 arrays (merge-based O(n+m))."""
    result = []
    i = j = 0
    while i < len(a) and j < len(b):
        if a[i] == b[j]:
            result.append(a[i])
            i += 1
            j += 1
        elif a[i] < b[j]:
            i += 1
        else:
            j += 1
    return np.array(result, dtype=np.int64)
