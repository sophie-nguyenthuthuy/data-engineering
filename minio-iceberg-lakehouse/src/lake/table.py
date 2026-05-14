"""Table — append-only commit machinery + time-travel + schema evolution."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from lake.manifest import FileStatus, Manifest, ManifestEntry
from lake.metadata import TableMetadata
from lake.snapshot import Snapshot, SnapshotOp
from lake.storage.base import CASMismatch

if TYPE_CHECKING:
    from collections.abc import Iterable

    from lake.datafile import DataFile
    from lake.schema import Schema
    from lake.storage.base import Storage


class TableError(RuntimeError):
    """Raised when a table operation cannot be applied."""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _metadata_path(uuid_: str, version: int) -> str:
    return f"metadata/{uuid_}/v{version}.json"


def _manifest_path(uuid_: str, manifest_id: str) -> str:
    return f"metadata/{uuid_}/manifests/{manifest_id}.json"


def _serialise_metadata(meta: TableMetadata) -> bytes:
    return json.dumps(
        {
            "table_uuid": meta.table_uuid,
            "location": meta.location,
            "current_schema_id": meta.current_schema_id,
            "current_snapshot_id": meta.current_snapshot_id,
            "last_updated_ms": meta.last_updated_ms,
            "schemas": [
                {
                    "schema_id": s.schema_id,
                    "fields": [
                        {"id": f.id, "name": f.name, "type": f.type.value, "required": f.required}
                        for f in s.fields
                    ],
                }
                for s in meta.schemas
            ],
            "snapshots": [
                {
                    "snapshot_id": sn.snapshot_id,
                    "parent_id": sn.parent_id,
                    "timestamp_ms": sn.timestamp_ms,
                    "op": sn.op.value,
                    "schema_id": sn.schema_id,
                    "manifest_ids": list(sn.manifest_ids),
                    "summary": sn.summary,
                }
                for sn in meta.snapshots
            ],
        },
        sort_keys=True,
    ).encode("utf-8")


def _serialise_manifest(manifest: Manifest) -> bytes:
    return json.dumps(
        {
            "manifest_id": manifest.manifest_id,
            "entries": [
                {
                    "status": e.status.value,
                    "file": {
                        "path": e.file.path,
                        "record_count": e.file.record_count,
                        "file_size_bytes": e.file.file_size_bytes,
                        "partition": e.file.partition,
                        "column_min": {str(k): v for k, v in e.file.column_min.items()},
                        "column_max": {str(k): v for k, v in e.file.column_max.items()},
                        "null_counts": {str(k): v for k, v in e.file.null_counts.items()},
                    },
                }
                for e in manifest.entries
            ],
        },
        sort_keys=True,
    ).encode("utf-8")


@dataclass
class Table:
    """One Iceberg-like table backed by a :class:`Storage`."""

    storage: Storage
    metadata: TableMetadata
    metadata_version: int = 1
    _metadata_etag: str | None = None
    _next_snapshot_id: int = field(default=1, init=False, repr=False)

    def __post_init__(self) -> None:
        # Snapshot IDs start after the largest already on disk.
        self._next_snapshot_id = (
            max((s.snapshot_id for s in self.metadata.snapshots), default=0) + 1
        )

    # ---------------------------------------------------------------- API

    @classmethod
    def create(
        cls,
        *,
        storage: Storage,
        location: str,
        initial_schema: Schema,
    ) -> Table:
        meta = TableMetadata(
            table_uuid=str(uuid.uuid4()),
            location=location,
            schemas=(initial_schema,),
            current_schema_id=initial_schema.schema_id,
            snapshots=(),
            current_snapshot_id=None,
            last_updated_ms=_now_ms(),
        )
        path = _metadata_path(meta.table_uuid, 1)
        etag = storage.atomic_put(path, _serialise_metadata(meta), expected_etag=None)
        tbl = cls(storage=storage, metadata=meta, metadata_version=1)
        tbl._metadata_etag = etag
        return tbl

    # ---------------------------------------------------------- commits

    def append(self, files: Iterable[DataFile]) -> Snapshot:
        return self._commit(SnapshotOp.APPEND, added=list(files), deleted=[])

    def delete(self, files: Iterable[DataFile]) -> Snapshot:
        return self._commit(SnapshotOp.DELETE, added=[], deleted=list(files))

    def overwrite(self, files: Iterable[DataFile]) -> Snapshot:
        # Find every currently-live file and mark it deleted in the same commit.
        live = self._live_files()
        return self._commit(SnapshotOp.OVERWRITE, added=list(files), deleted=live)

    # ------------------------------------------------------ schema evolution

    def evolve_schema(self, new_schema: Schema) -> None:
        new_meta = self.metadata.with_schema(new_schema)
        self._swap_metadata(new_meta)

    # ---------------------------------------------------------- time travel

    def at_snapshot(self, snap_id: int) -> Snapshot:
        return self.metadata.snapshot(snap_id)

    def files_at(self, snap_id: int) -> list[DataFile]:
        """Reconstruct the live file set as of ``snap_id`` by replaying
        every snapshot from the table's head back to the root, additively
        on the historical path leading to ``snap_id``."""
        chain = self._snapshot_chain(snap_id)
        live: dict[str, DataFile] = {}
        for sn in chain:
            for manifest_id in sn.manifest_ids:
                manifest = self._read_manifest(manifest_id)
                for entry in manifest.entries:
                    if entry.status is FileStatus.ADDED:
                        live[entry.file.path] = entry.file
                    elif entry.status is FileStatus.DELETED:
                        live.pop(entry.file.path, None)
        return list(live.values())

    def rollback(self, snap_id: int) -> None:
        new_meta = self.metadata.rollback_to(snap_id)
        self._swap_metadata(new_meta)

    # ----------------------------------------------------------- internals

    def _commit(
        self,
        op: SnapshotOp,
        *,
        added: list[DataFile],
        deleted: list[DataFile],
    ) -> Snapshot:
        if not added and not deleted:
            raise TableError("commit must add or delete at least one file")
        manifest_id = uuid.uuid4().hex
        entries = tuple(ManifestEntry(FileStatus.ADDED, f) for f in added) + tuple(
            ManifestEntry(FileStatus.DELETED, f) for f in deleted
        )
        manifest = Manifest(manifest_id=manifest_id, entries=entries)
        self.storage.put(
            _manifest_path(self.metadata.table_uuid, manifest_id),
            _serialise_manifest(manifest),
        )
        snap = Snapshot(
            snapshot_id=self._next_snapshot_id,
            parent_id=self.metadata.current_snapshot_id,
            timestamp_ms=_now_ms(),
            op=op,
            schema_id=self.metadata.current_schema_id,
            manifest_ids=(manifest_id,),
            summary={
                "added_files": len(added),
                "deleted_files": len(deleted),
                "added_records": sum(f.record_count for f in added),
            },
        )
        new_meta = self.metadata.with_snapshot(snap)
        self._swap_metadata(new_meta)
        return snap

    def _swap_metadata(self, new_meta: TableMetadata) -> None:
        next_version = self.metadata_version + 1
        next_path = _metadata_path(new_meta.table_uuid, next_version)
        try:
            etag = self.storage.atomic_put(
                next_path, _serialise_metadata(new_meta), expected_etag=None
            )
        except CASMismatch as exc:
            raise TableError(f"concurrent metadata write at v{next_version}: {exc}") from exc
        self.metadata = new_meta
        self.metadata_version = next_version
        self._metadata_etag = etag
        self._next_snapshot_id = max((s.snapshot_id for s in new_meta.snapshots), default=0) + 1

    def _read_manifest(self, manifest_id: str) -> Manifest:
        path = _manifest_path(self.metadata.table_uuid, manifest_id)
        raw = self.storage.get(path)
        obj = json.loads(raw.decode("utf-8"))
        from lake.datafile import DataFile

        entries = tuple(
            ManifestEntry(
                status=FileStatus(e["status"]),
                file=DataFile(
                    path=e["file"]["path"],
                    record_count=e["file"]["record_count"],
                    file_size_bytes=e["file"]["file_size_bytes"],
                    partition=e["file"]["partition"],
                    column_min={int(k): v for k, v in e["file"]["column_min"].items()},
                    column_max={int(k): v for k, v in e["file"]["column_max"].items()},
                    null_counts={int(k): v for k, v in e["file"]["null_counts"].items()},
                ),
            )
            for e in obj["entries"]
        )
        return Manifest(manifest_id=obj["manifest_id"], entries=entries)

    def _snapshot_chain(self, leaf_id: int) -> list[Snapshot]:
        # Walk parent pointers from the root to ``leaf_id`` so replay is in
        # chronological order (oldest commit first).
        chain: list[Snapshot] = []
        cur = self.metadata.snapshot(leaf_id)
        while True:
            chain.append(cur)
            if cur.parent_id is None:
                break
            cur = self.metadata.snapshot(cur.parent_id)
        chain.reverse()
        return chain

    def _live_files(self) -> list[DataFile]:
        if self.metadata.current_snapshot_id is None:
            return []
        return self.files_at(self.metadata.current_snapshot_id)


__all__ = ["Table", "TableError"]
