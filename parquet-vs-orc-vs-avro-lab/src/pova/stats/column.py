"""Per-column statistics used for predicate pushdown."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ColumnStats:
    """Min, max, null-count of a column or column chunk."""

    min: Any | None
    max: Any | None
    null_count: int
    n_rows: int

    def __post_init__(self) -> None:
        if self.null_count < 0:
            raise ValueError("null_count must be ≥ 0")
        if self.n_rows < 0:
            raise ValueError("n_rows must be ≥ 0")
        if self.null_count > self.n_rows:
            raise ValueError("null_count cannot exceed n_rows")

    @classmethod
    def from_values(cls, values: list[Any]) -> ColumnStats:
        if not values:
            return cls(min=None, max=None, null_count=0, n_rows=0)
        non_null = [v for v in values if v is not None]
        if not non_null:
            return cls(min=None, max=None, null_count=len(values), n_rows=len(values))
        return cls(
            min=min(non_null),
            max=max(non_null),
            null_count=len(values) - len(non_null),
            n_rows=len(values),
        )


__all__ = ["ColumnStats"]
