"""Iceberg-shaped table — a compact slice of the same model as project 11.

We use Iceberg's *snapshot tree* directly: each commit produces a new
:class:`Snapshot` pointing at a parent + a list of ``(file, ADDED |
DELETED)`` entries. Time-travel and rollback walk that tree.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tfl.delta.action import FileEntry


class FileStatus(str, Enum):
    ADDED = "added"
    DELETED = "deleted"


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    status: FileStatus
    file: FileEntry


@dataclass(frozen=True, slots=True)
class Snapshot:
    """One immutable version pointer."""

    snapshot_id: int
    parent_id: int | None
    entries: tuple[ManifestEntry, ...]


@dataclass
class IcebergTable:
    """In-memory Iceberg-shaped table."""

    _snapshots: list[Snapshot] = field(default_factory=list, repr=False)
    _current: int | None = field(default=None, repr=False)
    _next_id: int = field(default=1, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    # ----------------------------------------------------------- commits

    def append(self, files: list[FileEntry]) -> int:
        return self._commit([ManifestEntry(FileStatus.ADDED, f) for f in files])

    def delete(self, files: list[FileEntry]) -> int:
        return self._commit([ManifestEntry(FileStatus.DELETED, f) for f in files])

    def overwrite(self, files: list[FileEntry]) -> int:
        with self._lock:
            live = self.files_at()
            return self._commit(
                [ManifestEntry(FileStatus.DELETED, f) for f in live]
                + [ManifestEntry(FileStatus.ADDED, f) for f in files]
            )

    def _commit(self, entries: list[ManifestEntry]) -> int:
        if not entries:
            raise ValueError("commit requires ≥ 1 entry")
        with self._lock:
            snap = Snapshot(
                snapshot_id=self._next_id,
                parent_id=self._current,
                entries=tuple(entries),
            )
            self._snapshots.append(snap)
            self._current = snap.snapshot_id
            self._next_id += 1
            return snap.snapshot_id

    # ------------------------------------------------------ time travel

    def current(self) -> int | None:
        with self._lock:
            return self._current

    def snapshots(self) -> list[Snapshot]:
        with self._lock:
            return list(self._snapshots)

    def rollback(self, snap_id: int) -> None:
        with self._lock:
            if not any(s.snapshot_id == snap_id for s in self._snapshots):
                raise KeyError(f"unknown snapshot {snap_id}")
            self._current = snap_id

    def files_at(self, snap_id: int | None = None) -> list[FileEntry]:
        target = self._current if snap_id is None else snap_id
        if target is None:
            return []
        with self._lock:
            chain: list[Snapshot] = []
            by_id = {s.snapshot_id: s for s in self._snapshots}
            cur: Snapshot | None = by_id.get(target)
            while cur is not None:
                chain.append(cur)
                cur = by_id.get(cur.parent_id) if cur.parent_id is not None else None
            chain.reverse()
            live: dict[str, FileEntry] = {}
            for snap in chain:
                for e in snap.entries:
                    if e.status is FileStatus.ADDED:
                        live[e.file.path] = e.file
                    else:
                        live.pop(e.file.path, None)
            return list(live.values())


__all__ = ["FileStatus", "IcebergTable", "ManifestEntry", "Snapshot"]
