"""Workload profile assembled from observed queries.

We model a query as a conjunction of column predicates, each one either an
equality (``"="``) or a closed range (``"range"``). The profile tracks:

  * per-column access count (equality vs range)
  * per-column average selectivity (range width / domain width)
  * pairwise co-occurrence (which columns appear together)

These are exactly the signals a layout policy needs to decide between a
sort-key, Z-order, or Hilbert layout.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Literal, TypeAlias

EqPred: TypeAlias = tuple[Literal["="], float]
RangePred: TypeAlias = tuple[Literal["range"], float, float]
Predicate: TypeAlias = EqPred | RangePred


@dataclass(frozen=True, slots=True)
class Query:
    """A read query as a mapping ``column -> predicate``."""

    predicates: dict[str, Predicate]

    def columns(self) -> frozenset[str]:
        return frozenset(self.predicates)


@dataclass
class WorkloadProfile:
    """Mutable workload summary updated query-by-query."""

    columns: list[str]
    n: int = 0
    eq_count: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    range_count: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    range_selectivity_sum: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    correlation: Counter[frozenset[str]] = field(default_factory=Counter)
    domain: dict[str, tuple[float, float]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("columns must be non-empty")
        if len(set(self.columns)) != len(self.columns):
            raise ValueError("duplicate column names")

    def set_domain(self, col: str, lo: float, hi: float) -> None:
        if col not in self.columns:
            raise ValueError(f"unknown column {col!r}")
        if hi <= lo:
            raise ValueError("hi must be > lo")
        self.domain[col] = (lo, hi)

    def observe(self, q: Query) -> None:
        self.n += 1
        cols = list(q.predicates.keys())
        for c, pred in q.predicates.items():
            if c not in self.columns:
                raise ValueError(f"query references unknown column {c!r}")
            if pred[0] == "=":
                self.eq_count[c] += 1
            else:
                self.range_count[c] += 1
                lo, hi = pred[1], pred[2]
                dom = self.domain.get(c)
                if dom is not None:
                    width = max(hi - lo, 0.0) / (dom[1] - dom[0])
                    self.range_selectivity_sum[c] += min(width, 1.0)
                else:
                    self.range_selectivity_sum[c] += 1.0
        for i, c1 in enumerate(cols):
            for c2 in cols[i + 1 :]:
                self.correlation[frozenset({c1, c2})] += 1

    def freq(self, col: str) -> float:
        if self.n == 0:
            return 0.0
        return (self.eq_count[col] + self.range_count[col]) / self.n

    def range_fraction(self, col: str) -> float:
        total = self.eq_count[col] + self.range_count[col]
        if total == 0:
            return 0.0
        return self.range_count[col] / total

    def mean_selectivity(self, col: str) -> float:
        if self.range_count[col] == 0:
            return 0.0
        return self.range_selectivity_sum[col] / self.range_count[col]

    def top_cols(self, k: int = 4) -> list[str]:
        scored = sorted(self.columns, key=lambda c: -self.freq(c))
        return [c for c in scored if self.freq(c) > 0][:k]

    def co_occurrence(self, a: str, b: str) -> int:
        return self.correlation.get(frozenset({a, b}), 0)

    def snapshot(self) -> dict[str, float]:
        """Flat feature vector keyed by ``"freq:<col>"`` / ``"range:<col>"``."""
        out: dict[str, float] = {}
        for c in self.columns:
            out[f"freq:{c}"] = self.freq(c)
            out[f"range:{c}"] = self.range_fraction(c)
        return out


__all__ = ["Query", "WorkloadProfile"]
