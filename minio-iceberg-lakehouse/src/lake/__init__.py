"""minio-iceberg-lakehouse — from-scratch Iceberg-style table format."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from lake.catalog import Catalog, CatalogError
    from lake.datafile import DataFile
    from lake.manifest import Manifest
    from lake.metadata import TableMetadata
    from lake.schema import Field, FieldType, Schema, SchemaEvolutionError
    from lake.snapshot import Snapshot, SnapshotOp
    from lake.storage.base import Storage
    from lake.storage.inmemory import InMemoryStorage
    from lake.storage.local_fs import LocalFSStorage
    from lake.table import Table, TableError


_LAZY: dict[str, tuple[str, str]] = {
    "Field": ("lake.schema", "Field"),
    "FieldType": ("lake.schema", "FieldType"),
    "Schema": ("lake.schema", "Schema"),
    "SchemaEvolutionError": ("lake.schema", "SchemaEvolutionError"),
    "DataFile": ("lake.datafile", "DataFile"),
    "Manifest": ("lake.manifest", "Manifest"),
    "Snapshot": ("lake.snapshot", "Snapshot"),
    "SnapshotOp": ("lake.snapshot", "SnapshotOp"),
    "TableMetadata": ("lake.metadata", "TableMetadata"),
    "Storage": ("lake.storage.base", "Storage"),
    "InMemoryStorage": ("lake.storage.inmemory", "InMemoryStorage"),
    "LocalFSStorage": ("lake.storage.local_fs", "LocalFSStorage"),
    "Table": ("lake.table", "Table"),
    "TableError": ("lake.table", "TableError"),
    "Catalog": ("lake.catalog", "Catalog"),
    "CatalogError": ("lake.catalog", "CatalogError"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Catalog",
    "CatalogError",
    "DataFile",
    "Field",
    "FieldType",
    "InMemoryStorage",
    "LocalFSStorage",
    "Manifest",
    "Schema",
    "SchemaEvolutionError",
    "Snapshot",
    "SnapshotOp",
    "Storage",
    "Table",
    "TableError",
    "TableMetadata",
    "__version__",
]
