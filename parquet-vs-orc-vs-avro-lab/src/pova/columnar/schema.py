"""Schema = ordered list of (name, type) pairs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pova.columnar.column import Column, ColumnType


@dataclass(frozen=True, slots=True)
class Schema:
    """Ordered column declarations (name, type) — duplicate names rejected."""

    fields: tuple[tuple[str, ColumnType], ...]

    def __post_init__(self) -> None:
        if not self.fields:
            raise ValueError("schema must have at least one field")
        seen: set[str] = set()
        for name, _ in self.fields:
            if not name:
                raise ValueError("field name must be non-empty")
            if name in seen:
                raise ValueError(f"duplicate field name {name!r}")
            seen.add(name)

    def __len__(self) -> int:
        return len(self.fields)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.fields)

    def validate(self, columns: list[Column]) -> None:
        if len(columns) != len(self.fields):
            raise ValueError(f"schema has {len(self.fields)} fields but got {len(columns)} columns")
        for (name, ctype), col in zip(self.fields, columns, strict=True):
            if col.name != name:
                raise ValueError(f"column #{name!r} mismatch: got {col.name!r}")
            if col.type is not ctype:
                raise ValueError(f"column {name!r} expected {ctype.value} but got {col.type.value}")
        # Every column must have the same row count.
        if len({len(c) for c in columns}) > 1:
            raise ValueError("columns have differing lengths")


__all__ = ["Schema"]
