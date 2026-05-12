"""Workload profile from query logs.

Each query is a predicate over columns. The profile tracks per-column access
frequency, range vs equality, and pairwise predicate correlation.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field


@dataclass
class Query:
    predicates: dict     # column_name -> ('=', value) or ('range', lo, hi)


@dataclass
class WorkloadProfile:
    columns: list
    n: int = 0
    eq_count: dict = field(default_factory=lambda: defaultdict(int))
    range_count: dict = field(default_factory=lambda: defaultdict(int))
    correlation: dict = field(default_factory=Counter)   # frozenset({a,b}) -> count

    def observe(self, q: Query) -> None:
        self.n += 1
        preds = set(q.predicates.keys())
        for c, pred in q.predicates.items():
            if pred[0] == "=":
                self.eq_count[c] += 1
            else:
                self.range_count[c] += 1
        for c1 in preds:
            for c2 in preds:
                if c1 < c2:
                    self.correlation[frozenset({c1, c2})] += 1

    def freq(self, col: str) -> float:
        return (self.eq_count[col] + self.range_count[col]) / max(self.n, 1)

    def range_fraction(self, col: str) -> float:
        total = self.eq_count[col] + self.range_count[col]
        return self.range_count[col] / max(total, 1)

    def top_cols(self, k: int = 4) -> list:
        scored = [(c, self.freq(c)) for c in self.columns]
        scored.sort(key=lambda x: -x[1])
        return [c for c, _ in scored[:k]]


__all__ = ["Query", "WorkloadProfile"]
