from __future__ import annotations
from datetime import datetime
from typing import Any
from pydantic import BaseModel, ConfigDict


# ── DataSource ──────────────────────────────────────────────────────────────

class DataSourceCreate(BaseModel):
    name: str
    engine_type: str
    connection_string: str
    description: str | None = None


class DataSourceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    engine_type: str
    description: str | None
    created_at: datetime
    last_scanned_at: datetime | None


# ── Column ───────────────────────────────────────────────────────────────────

class ColumnOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    data_type: str | None
    is_nullable: bool
    is_primary_key: bool
    description: str | None
    pii_tags: list[str]
    sample_values: list[str]


class ColumnUpdate(BaseModel):
    description: str | None = None
    pii_tags: list[str] | None = None


# ── Table ────────────────────────────────────────────────────────────────────

class TableOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    description: str | None
    row_count: int | None
    tags: list[str]
    columns: list[ColumnOut] = []


# ── Schema ───────────────────────────────────────────────────────────────────

class SchemaOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    description: str | None
    tables: list[TableOut] = []


# ── LineageJob ────────────────────────────────────────────────────────────────

class LineageJobCreate(BaseModel):
    name: str
    description: str | None = None
    sql_query: str
    job_type: str = "sql"
    dialect: str = "sqlite"
    tags: list[str] = []


class LineageJobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    description: str | None
    sql_query: str
    job_type: str
    tags: list[str]
    created_at: datetime
    updated_at: datetime


# ── Lineage graph ────────────────────────────────────────────────────────────

class LineageNode(BaseModel):
    id: str
    label: str
    type: str  # "column" | "table"
    pii_tags: list[str] = []
    source_name: str | None = None


class LineageEdge(BaseModel):
    id: str
    source: str
    target: str
    job_id: int
    job_name: str
    transform: str | None = None


class LineageGraph(BaseModel):
    nodes: list[LineageNode]
    edges: list[LineageEdge]


# ── Search ────────────────────────────────────────────────────────────────────

class SearchResult(BaseModel):
    type: str          # "table" | "column"
    source_name: str
    schema_name: str
    table_name: str
    column_name: str | None
    pii_tags: list[str]
    id: int
