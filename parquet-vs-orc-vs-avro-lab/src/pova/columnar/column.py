"""Typed column container."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class ColumnType(str, Enum):
    """Subset of column types the lab supports."""

    INT64 = "int64"
    FLOAT64 = "float64"
    STRING = "string"
    BOOL = "bool"


_PY_TYPE = {
    ColumnType.INT64: int,
    ColumnType.FLOAT64: float,
    ColumnType.STRING: str,
    ColumnType.BOOL: bool,
}


@dataclass(frozen=True, slots=True)
class Column:
    """A single named column of typed values (nulls allowed)."""

    name: str
    type: ColumnType
    values: tuple[Any, ...]

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name must be non-empty")
        pytype = _PY_TYPE[self.type]
        # Bool is a subclass of int — be strict so the column dtype is honest.
        for i, v in enumerate(self.values):
            if v is None:
                continue
            if self.type is ColumnType.INT64 and isinstance(v, bool):
                raise TypeError(f"int64 column {self.name!r} value {i} is a bool")
            if self.type is ColumnType.FLOAT64 and not isinstance(v, int | float):
                raise TypeError(f"float64 column {self.name!r} value {i} is {type(v).__name__}")
            if self.type is not ColumnType.FLOAT64 and not isinstance(v, pytype):
                raise TypeError(
                    f"column {self.name!r} value {i} is {type(v).__name__}, expected {pytype.__name__}"
                )

    def __len__(self) -> int:
        return len(self.values)

    def null_count(self) -> int:
        return sum(1 for v in self.values if v is None)


__all__ = ["Column", "ColumnType"]
