"""Snapshot = one logical version of the table.

Each snapshot points at:
  * a parent snapshot id (NULL for the first one),
  * a list of manifest ids that *together* describe the table at this
    point in time,
  * a summary of records added / deleted in this commit,
  * the schema id active at commit time.

A consumer doing time travel picks a snapshot and reads its manifests
— that's the entire mechanism.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class SnapshotOp(str, Enum):
    """What kind of commit produced this snapshot."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    DELETE = "delete"


@dataclass(frozen=True, slots=True)
class Snapshot:
    """One immutable version pointer."""

    snapshot_id: int
    parent_id: int | None
    timestamp_ms: int
    op: SnapshotOp
    schema_id: int
    manifest_ids: tuple[str, ...]
    summary: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.snapshot_id < 1:
            raise ValueError("snapshot_id must be ≥ 1")
        if self.parent_id is not None and self.parent_id < 1:
            raise ValueError("parent_id must be ≥ 1 or None")
        if self.timestamp_ms < 0:
            raise ValueError("timestamp_ms must be ≥ 0")
        if self.schema_id < 0:
            raise ValueError("schema_id must be ≥ 0")


__all__ = ["Snapshot", "SnapshotOp"]
