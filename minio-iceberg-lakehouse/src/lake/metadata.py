"""TableMetadata — the single file the catalog points at.

In real Iceberg this is the JSON file whose path the Hive Metastore /
Glue / Polaris stores. Our implementation matches that shape: the
table's full history (schema changes + snapshot chain) is materialised
into one immutable record, and a commit produces a *new* immutable
record swapped atomically by the storage layer.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lake.schema import Schema
    from lake.snapshot import Snapshot


@dataclass(frozen=True, slots=True)
class TableMetadata:
    """Snapshot history + schema history + current pointer."""

    table_uuid: str
    location: str
    schemas: tuple[Schema, ...]
    current_schema_id: int
    snapshots: tuple[Snapshot, ...]
    current_snapshot_id: int | None
    last_updated_ms: int = field(default=0)

    def __post_init__(self) -> None:
        if not self.table_uuid:
            raise ValueError("table_uuid must be non-empty")
        if not self.location:
            raise ValueError("location must be non-empty")
        if not self.schemas:
            raise ValueError("schemas must be non-empty")
        schema_ids = {s.schema_id for s in self.schemas}
        if self.current_schema_id not in schema_ids:
            raise ValueError(f"current_schema_id {self.current_schema_id} not in schemas")
        snap_ids = {s.snapshot_id for s in self.snapshots}
        if self.current_snapshot_id is not None and self.current_snapshot_id not in snap_ids:
            raise ValueError(f"current_snapshot_id {self.current_snapshot_id} not in snapshots")

    # ----------------------------------------------------------- access

    def schema(self, sid: int | None = None) -> Schema:
        target = self.current_schema_id if sid is None else sid
        for s in self.schemas:
            if s.schema_id == target:
                return s
        raise KeyError(f"schema {target} not found")

    def current_snapshot(self) -> Snapshot | None:
        if self.current_snapshot_id is None:
            return None
        for s in self.snapshots:
            if s.snapshot_id == self.current_snapshot_id:
                return s
        return None

    def snapshot(self, sid: int) -> Snapshot:
        for s in self.snapshots:
            if s.snapshot_id == sid:
                return s
        raise KeyError(f"snapshot {sid} not found")

    def with_schema(self, new_schema: Schema) -> TableMetadata:
        if any(s.schema_id == new_schema.schema_id for s in self.schemas):
            raise ValueError(f"schema_id {new_schema.schema_id} already present")
        return TableMetadata(
            table_uuid=self.table_uuid,
            location=self.location,
            schemas=(*self.schemas, new_schema),
            current_schema_id=new_schema.schema_id,
            snapshots=self.snapshots,
            current_snapshot_id=self.current_snapshot_id,
            last_updated_ms=int(time.time() * 1000),
        )

    def with_snapshot(self, snap: Snapshot) -> TableMetadata:
        if any(s.snapshot_id == snap.snapshot_id for s in self.snapshots):
            raise ValueError(f"snapshot_id {snap.snapshot_id} already present")
        return TableMetadata(
            table_uuid=self.table_uuid,
            location=self.location,
            schemas=self.schemas,
            current_schema_id=self.current_schema_id,
            snapshots=(*self.snapshots, snap),
            current_snapshot_id=snap.snapshot_id,
            last_updated_ms=int(time.time() * 1000),
        )

    def rollback_to(self, snap_id: int) -> TableMetadata:
        # We don't *delete* later snapshots — Iceberg's history keeps them
        # available for further time-travel.
        snap = self.snapshot(snap_id)
        return TableMetadata(
            table_uuid=self.table_uuid,
            location=self.location,
            schemas=self.schemas,
            current_schema_id=self.current_schema_id,
            snapshots=self.snapshots,
            current_snapshot_id=snap.snapshot_id,
            last_updated_ms=int(time.time() * 1000),
        )


__all__ = ["TableMetadata"]
