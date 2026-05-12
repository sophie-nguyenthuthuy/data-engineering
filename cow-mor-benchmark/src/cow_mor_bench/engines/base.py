"""Abstract base for storage engines."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa


@dataclass
class WriteResult:
    rows_written: int
    files_written: int
    files_rewritten: int       # CoW: rewrites; MoR: 0 for deltas
    bytes_written: int
    duration_s: float
    operation: str


@dataclass
class ReadResult:
    rows_returned: int
    files_scanned: int
    bytes_scanned: int
    delta_files_merged: int    # MoR: number of delta files merged
    duration_s: float
    query: str


@dataclass
class TableStats:
    data_file_count: int
    delta_file_count: int
    total_data_bytes: int
    total_delta_bytes: int
    total_rows: int
    amplification_ratio: float  # (data+delta)/data — 1.0 means no amplification


class StorageEngine(ABC):
    def __init__(self, table_path: str, schema_name: str):
        self.table_path = table_path
        self.schema_name = schema_name

    @abstractmethod
    def create_table(self, initial_data: pa.Table) -> WriteResult:
        ...

    @abstractmethod
    def insert(self, rows: pa.Table) -> WriteResult:
        ...

    @abstractmethod
    def update(self, updated_rows: pa.Table) -> WriteResult:
        ...

    @abstractmethod
    def delete(self, pk_values: list[int]) -> WriteResult:
        ...

    @abstractmethod
    def full_scan(self, filter_expr: str | None = None) -> ReadResult:
        ...

    @abstractmethod
    def point_lookup(self, pk_value: int) -> ReadResult:
        ...

    @abstractmethod
    def range_scan(self, pk_min: int, pk_max: int) -> ReadResult:
        ...

    @abstractmethod
    def stats(self) -> TableStats:
        ...

    @abstractmethod
    def compact(self) -> WriteResult:
        ...
