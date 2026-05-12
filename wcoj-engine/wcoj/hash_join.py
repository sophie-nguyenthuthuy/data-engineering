"""Classic multi-way hash join baseline.

Executes a left-deep chain of two-way hash joins in the order that relations
appear in the query.  This is the standard approach used by most RDBMS engines.

For acyclic queries this is nearly optimal; for cyclic queries (e.g. triangle
counting) it can produce results exponentially larger than the final output,
making it far slower than WCOJ algorithms.

Public API
----------
hash_join(query) → np.ndarray
"""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Tuple

import numpy as np

from .query import JoinQuery, Relation


def hash_join(query: JoinQuery) -> np.ndarray:
    """Run a left-deep multi-way hash join.

    Returns a 2-D int64 array whose columns correspond to the query's
    variable list (in appearance order).
    """
    if not query.relations:
        return np.empty((0, 0), dtype=np.int64)

    # Seed with the first relation.
    rel0 = query.relations[0]
    result_data = rel0.data.copy()
    result_vars: List[str] = list(rel0.variables)

    for rel in query.relations[1:]:
        result_data, result_vars = _two_way_hash_join(
            result_data, result_vars, rel.data, rel.variables
        )
        if len(result_data) == 0:
            # Early exit — no tuples remain.
            n_vars = len(result_vars) + sum(
                1 for v in rel.variables if v not in result_vars
            )
            return np.empty((0, n_vars), dtype=np.int64)

    # Re-order columns to match query.variables.
    query_vars = query.variables
    col_map = {v: i for i, v in enumerate(result_vars)}
    col_order = [col_map[v] for v in query_vars if v in col_map]
    return result_data[:, col_order]


# ------------------------------------------------------------------ #
#  Two-way hash join                                                  #
# ------------------------------------------------------------------ #

def _two_way_hash_join(
    left: np.ndarray,
    left_vars: List[str],
    right: np.ndarray,
    right_vars: List[str],
) -> Tuple[np.ndarray, List[str]]:
    """Hash join between left and right on their shared variables."""
    join_vars = [v for v in left_vars if v in right_vars]

    if not join_vars:
        # Cartesian product (rare in well-formed queries).
        return _cartesian(left, left_vars, right, right_vars)

    left_key_cols = [left_vars.index(v) for v in join_vars]
    right_key_cols = [right_vars.index(v) for v in join_vars]
    right_extra_cols = [i for i, v in enumerate(right_vars) if v not in join_vars]
    right_extra_vars = [right_vars[i] for i in right_extra_cols]

    # Build hash table on the smaller side (right here).
    ht: Dict[Tuple, List[np.ndarray]] = defaultdict(list)
    for row in right:
        key = tuple(int(row[c]) for c in right_key_cols)
        ht[key].append(row[right_extra_cols])

    # Probe.
    rows = []
    for left_row in left:
        key = tuple(int(left_row[c]) for c in left_key_cols)
        for extra in ht.get(key, ()):
            rows.append(np.concatenate([left_row, extra]))

    out_vars = left_vars + right_extra_vars
    if rows:
        return np.array(rows, dtype=np.int64), out_vars
    return np.empty((0, len(out_vars)), dtype=np.int64), out_vars


def _cartesian(
    left: np.ndarray,
    left_vars: List[str],
    right: np.ndarray,
    right_vars: List[str],
) -> Tuple[np.ndarray, List[str]]:
    n_l, n_r = len(left), len(right)
    out_vars = left_vars + right_vars
    if n_l == 0 or n_r == 0:
        return np.empty((0, len(out_vars)), dtype=np.int64), out_vars
    left_rep = np.repeat(left, n_r, axis=0)
    right_rep = np.tile(right, (n_l, 1))
    return np.concatenate([left_rep, right_rep], axis=1), out_vars
