"""Relational schema + per-column statistics for the cost model."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from ppc.ir.types import DataType


@dataclass(frozen=True, slots=True)
class Stats:
    """Per-column statistics used by the cost model.

    ndv:    number of distinct values (cardinality of the column)
    nulls:  null fraction in [0, 1]
    avg_len: average byte length for variable-width types
    """

    ndv: int | None = None
    nulls: float = 0.0
    avg_len: int | None = None

    def with_ndv(self, ndv: int | None) -> Stats:
        return Stats(ndv=ndv, nulls=self.nulls, avg_len=self.avg_len)


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    dtype: DataType
    stats: Stats = field(default_factory=Stats)

    def with_stats(self, stats: Stats) -> Column:
        return Column(name=self.name, dtype=self.dtype, stats=stats)


@dataclass(frozen=True, slots=True)
class Schema:
    """Ordered list of columns with O(1) name lookup."""

    columns: tuple[Column, ...]
    rows: int | None = None  # row-count estimate; None = unknown

    @classmethod
    def of(cls, *columns: Column, rows: int | None = None) -> Schema:
        return cls(columns=tuple(columns), rows=rows)

    def __iter__(self) -> Iterable[Column]:  # type: ignore[override]
        return iter(self.columns)

    def __len__(self) -> int:
        return len(self.columns)

    def __getitem__(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"column not found: {name}")

    def index(self, name: str) -> int:
        for i, c in enumerate(self.columns):
            if c.name == name:
                return i
        raise KeyError(f"column not found: {name}")

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    @property
    def row_width(self) -> int:
        return sum(c.dtype.byte_width for c in self.columns)

    def bytes_estimate(self) -> float:
        if self.rows is None:
            return float("nan")
        return self.rows * self.row_width

    def project(self, names: Iterable[str]) -> Schema:
        out = tuple(self[n] for n in names)
        return Schema(columns=out, rows=self.rows)

    def union(self, other: Schema) -> Schema:
        """Concatenate two schemas (join-style); deduplicate by name."""
        seen: set[str] = set()
        cols: list[Column] = []
        for c in (*self.columns, *other.columns):
            if c.name in seen:
                continue
            seen.add(c.name)
            cols.append(c)
        rows: int | None = None
        if self.rows is not None and other.rows is not None:
            # Cartesian upper bound; join refines it
            rows = self.rows * other.rows
        return Schema(columns=tuple(cols), rows=rows)

    def with_rows(self, rows: int | None) -> Schema:
        return Schema(columns=self.columns, rows=rows)
