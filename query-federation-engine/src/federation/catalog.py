"""Schema catalog — registers data sources and their table schemas."""

from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class SourceType(str, Enum):
    POSTGRES = "postgres"
    MONGODB = "mongodb"
    S3_PARQUET = "s3_parquet"
    REST_API = "rest_api"


@dataclass
class ColumnDef:
    name: str
    dtype: str  # "string", "int", "float", "bool", "timestamp"
    nullable: bool = True


@dataclass
class TableSchema:
    source: str          # source name, e.g. "postgres"
    table: str           # table name, e.g. "orders"
    source_type: SourceType
    columns: list[ColumnDef]
    estimated_rows: int = 100_000
    connection: dict[str, Any] = field(default_factory=dict)

    @property
    def qualified_name(self) -> str:
        return f"{self.source}.{self.table}"

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def get_column(self, name: str) -> ColumnDef | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None


class SchemaCatalog:
    """Registry of all federated tables across all data sources."""

    def __init__(self) -> None:
        self._tables: dict[str, TableSchema] = {}
        self._sources: dict[str, dict[str, Any]] = {}

    def register_source(self, name: str, source_type: SourceType, connection: dict[str, Any]) -> None:
        self._sources[name] = {"type": source_type, "connection": connection}

    def register_table(self, schema: TableSchema) -> None:
        self._tables[schema.qualified_name] = schema

    def get_table(self, qualified_name: str) -> TableSchema:
        if qualified_name not in self._tables:
            raise KeyError(f"Unknown table: {qualified_name!r}. Registered: {list(self._tables)}")
        return self._tables[qualified_name]

    def get_source_connection(self, source_name: str) -> dict[str, Any]:
        if source_name not in self._sources:
            raise KeyError(f"Unknown source: {source_name!r}")
        return self._sources[source_name]["connection"]

    def list_tables(self) -> list[str]:
        return list(self._tables.keys())

    @classmethod
    def from_yaml(cls, path: str | Path) -> "SchemaCatalog":
        catalog = cls()
        config = yaml.safe_load(Path(path).read_text())

        for src in config.get("sources", []):
            catalog.register_source(
                src["name"],
                SourceType(src["type"]),
                src.get("connection", {}),
            )

        for tbl in config.get("tables", []):
            source_name = tbl["source"]
            source_type = SourceType(catalog._sources[source_name]["type"])
            columns = [
                ColumnDef(c["name"], c["type"], c.get("nullable", True))
                for c in tbl.get("columns", [])
            ]
            schema = TableSchema(
                source=source_name,
                table=tbl["table"],
                source_type=source_type,
                columns=columns,
                estimated_rows=tbl.get("estimated_rows", 100_000),
                connection=catalog._sources[source_name]["connection"],
            )
            catalog.register_table(schema)

        return catalog
