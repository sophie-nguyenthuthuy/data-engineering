"""Catalog: tables → Schema. The optimizer needs both column types and stats."""

from __future__ import annotations

from dataclasses import dataclass, field

from ppc.ir.schema import Schema


@dataclass
class Catalog:
    """In-memory catalog of table_name -> Schema."""

    tables: dict[str, Schema] = field(default_factory=dict)

    def register(self, name: str, schema: Schema) -> None:
        if name in self.tables:
            raise ValueError(f"table already registered: {name}")
        self.tables[name] = schema

    def get(self, name: str) -> Schema:
        if name not in self.tables:
            raise KeyError(f"unknown table: {name}")
        return self.tables[name]

    def __contains__(self, name: str) -> bool:
        return name in self.tables
