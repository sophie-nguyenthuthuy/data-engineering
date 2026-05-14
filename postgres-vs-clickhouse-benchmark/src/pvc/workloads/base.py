"""Query + Workload primitives."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Query:
    """One benchmarked query."""

    id: str
    description: str
    sql: str

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("id must be non-empty")
        if not self.sql:
            raise ValueError("sql must be non-empty")
        if not self.description:
            raise ValueError("description must be non-empty")


@dataclass(frozen=True, slots=True)
class Workload:
    """A named collection of queries."""

    name: str
    queries: tuple[Query, ...]

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name must be non-empty")
        if not self.queries:
            raise ValueError("workload must have ≥ 1 query")
        seen: set[str] = set()
        for q in self.queries:
            if q.id in seen:
                raise ValueError(f"duplicate query id {q.id!r}")
            seen.add(q.id)

    def __len__(self) -> int:
        return len(self.queries)

    def by_id(self, qid: str) -> Query:
        for q in self.queries:
            if q.id == qid:
                return q
        raise KeyError(f"unknown query id {qid!r}")


__all__ = ["Query", "Workload"]
