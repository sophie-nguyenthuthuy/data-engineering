"""In-memory table catalog backed by Apache Arrow Tables."""
from __future__ import annotations

import pyarrow as pa
from typing import Dict, Optional


class Table:
    def __init__(self, name: str, data: pa.Table) -> None:
        self.name = name
        self.data = data

    @property
    def schema(self) -> pa.Schema:
        return self.data.schema

    @property
    def num_rows(self) -> int:
        return self.data.num_rows

    def __repr__(self) -> str:
        return f"Table({self.name!r}, rows={self.num_rows}, schema={self.schema})"


class Catalog:
    def __init__(self) -> None:
        self._tables: Dict[str, Table] = {}

    def register(self, name: str, data: pa.Table) -> None:
        self._tables[name.lower()] = Table(name.lower(), data)

    def get(self, name: str) -> Table:
        key = name.lower()
        if key not in self._tables:
            raise KeyError(f"Table {name!r} not found; available: {list(self._tables)}")
        return self._tables[key]

    def tables(self) -> list[str]:
        return list(self._tables.keys())
