"""Profile a query log into per-column usage counters."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from psa.parser import parse_query

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(frozen=True, slots=True)
class ColumnUsage:
    """How often a column appears in each query-clause kind."""

    name: str
    filter_count: int
    join_count: int
    group_count: int

    def total(self) -> int:
        return self.filter_count + self.join_count + self.group_count


@dataclass(frozen=True, slots=True)
class QueryProfile:
    """Aggregate usage counts across a query log."""

    n_queries: int
    columns: tuple[ColumnUsage, ...]

    def by_name(self) -> dict[str, ColumnUsage]:
        return {c.name: c for c in self.columns}

    def top_filter_columns(self, k: int = 5) -> list[ColumnUsage]:
        return sorted(self.columns, key=lambda c: -c.filter_count)[:k]

    def top_join_columns(self, k: int = 5) -> list[ColumnUsage]:
        return sorted(self.columns, key=lambda c: -c.join_count)[:k]


@dataclass
class Profiler:
    """Accumulate :class:`ColumnUsage` across a sequence of SQL strings."""

    _filter: Counter[str] = field(default_factory=Counter, repr=False)
    _join: Counter[str] = field(default_factory=Counter, repr=False)
    _group: Counter[str] = field(default_factory=Counter, repr=False)
    _n_queries: int = 0

    def consume(self, queries: Iterable[str]) -> None:
        for sql in queries:
            self.add(sql)

    def add(self, sql: str) -> None:
        parsed = parse_query(sql)
        for col in parsed.filter_columns:
            self._filter[col] += 1
        for col in parsed.join_columns:
            self._join[col] += 1
        for col in parsed.group_columns:
            self._group[col] += 1
        self._n_queries += 1

    def build(self) -> QueryProfile:
        names = sorted(set(self._filter) | set(self._join) | set(self._group))
        usages = tuple(
            ColumnUsage(
                name=n,
                filter_count=self._filter[n],
                join_count=self._join[n],
                group_count=self._group[n],
            )
            for n in names
        )
        return QueryProfile(n_queries=self._n_queries, columns=usages)


__all__ = ["ColumnUsage", "Profiler", "QueryProfile"]
