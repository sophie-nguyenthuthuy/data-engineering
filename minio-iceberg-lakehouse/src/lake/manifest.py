"""Manifest = group of data files added/deleted in one operation.

Iceberg manifests are Avro files; we represent them as plain
dataclasses + JSON on-disk so the metadata is human-readable. The
shape — list of (data file, status) pairs — is identical.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lake.datafile import DataFile


class FileStatus(str, Enum):
    """Was this file added, kept (existing), or deleted in this manifest."""

    ADDED = "added"
    EXISTING = "existing"
    DELETED = "deleted"


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """One data file + the action this manifest records."""

    status: FileStatus
    file: DataFile


@dataclass(frozen=True, slots=True)
class Manifest:
    """Group of entries written in a single commit."""

    manifest_id: str
    entries: tuple[ManifestEntry, ...]

    def __post_init__(self) -> None:
        if not self.manifest_id:
            raise ValueError("manifest_id must be non-empty")

    def added_files(self) -> tuple[DataFile, ...]:
        return tuple(e.file for e in self.entries if e.status is FileStatus.ADDED)

    def deleted_files(self) -> tuple[DataFile, ...]:
        return tuple(e.file for e in self.entries if e.status is FileStatus.DELETED)


__all__ = ["FileStatus", "Manifest", "ManifestEntry"]
