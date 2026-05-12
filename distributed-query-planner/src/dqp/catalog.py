"""Catalog: registry of tables and their schemas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class ColumnSchema:
    """Schema for a single column."""

    name: str
    dtype: str  # int, float, str, bool, date, datetime
    nullable: bool = True
    primary_key: bool = False

    def __post_init__(self) -> None:
        valid = {"int", "float", "str", "bool", "date", "datetime"}
        if self.dtype not in valid:
            raise ValueError(f"Invalid column dtype {self.dtype!r}")


@dataclass
class TableSchema:
    """Schema for a table, including which engine stores it."""

    name: str
    engine_name: str
    columns: List[ColumnSchema]
    row_count_hint: Optional[int] = None

    def get_column(self, name: str) -> Optional[ColumnSchema]:
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]


class Catalog:
    """Registry of all known tables and their schemas.

    Thread-safe for read-heavy workloads; write operations (register) should
    happen at startup.
    """

    def __init__(self) -> None:
        self._tables: Dict[str, TableSchema] = {}

    def register_table(self, schema: TableSchema) -> None:
        """Register (or replace) a table schema."""
        self._tables[schema.name] = schema

    def get_table(self, name: str) -> TableSchema:
        """Return the schema for *name*, raising KeyError if absent."""
        if name not in self._tables:
            raise KeyError(f"Table {name!r} not found in catalog")
        return self._tables[name]

    def list_tables(self) -> List[str]:
        """Return a sorted list of registered table names."""
        return sorted(self._tables.keys())

    def __repr__(self) -> str:
        tables = ", ".join(self.list_tables())
        return f"Catalog(tables=[{tables}])"
