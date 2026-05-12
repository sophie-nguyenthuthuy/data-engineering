"""Query representation: relations, join queries, variable metadata."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import List, Optional

import numpy as np


@dataclass
class Relation:
    """A relation (table) backed by a sorted numpy array.

    data is a 2-D int64 array sorted lexicographically by (variables[0],
    variables[1], ...).  The sort order must match the column order so that
    TrieIterator can perform binary search correctly.
    """

    name: str
    variables: List[str]
    data: np.ndarray  # shape (n_tuples, len(variables)), sorted lex

    def __post_init__(self) -> None:
        self.data = np.asarray(self.data, dtype=np.int64)
        if self.data.ndim == 1:
            self.data = self.data.reshape(-1, 1)
        assert self.data.shape[1] == len(self.variables), (
            f"Relation {self.name}: data has {self.data.shape[1]} columns "
            f"but {len(self.variables)} variables"
        )

    def sorted_by(self, var_order: List[str]) -> "Relation":
        """Return a copy of this relation with columns reordered and rows
        re-sorted to match *var_order* (restricted to this relation's vars)."""
        local_order = [v for v in var_order if v in self.variables]
        col_idx = [self.variables.index(v) for v in local_order]
        new_data = self.data[:, col_idx]
        # Lexicographic sort
        sort_keys = tuple(new_data[:, i] for i in range(len(col_idx) - 1, -1, -1))
        order = np.lexsort(sort_keys)
        return Relation(self.name, local_order, new_data[order])

    def __len__(self) -> int:
        return len(self.data)

    def __repr__(self) -> str:
        return f"Relation({self.name}, vars={self.variables}, rows={len(self.data)})"


@dataclass
class JoinQuery:
    """A natural join query over a list of relations."""

    relations: List[Relation]

    @property
    def variables(self) -> List[str]:
        """All variables in appearance order (no duplicates)."""
        seen: set = set()
        result = []
        for r in self.relations:
            for v in r.variables:
                if v not in seen:
                    seen.add(v)
                    result.append(v)
        return result

    def variable_order(self) -> List[str]:
        """Heuristic variable ordering: most-constrained first.

        Variables appearing in more relations are placed first — this
        maximises pruning early in the Leapfrog Triejoin recursion.
        """
        freq = Counter(v for r in self.relations for v in r.variables)
        return sorted(self.variables, key=lambda v: (-freq[v], v))

    def prepare_for_lftj(self, var_order: Optional[List[str]] = None) -> List[Relation]:
        """Return relations with columns/rows sorted according to *var_order*."""
        if var_order is None:
            var_order = self.variable_order()
        return [r.sorted_by(var_order) for r in self.relations]
