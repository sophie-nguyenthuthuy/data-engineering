"""Delta Lake transaction-log actions.

Every commit appends one entry (= a list of *actions*) to
``_delta_log/<NNNNNNNNNNNNNNNNNNNN>.json``. Each action is one of
``Metadata``, ``Add``, ``Remove``, or ``Commit``; together they
describe how the table moved from version ``n−1`` to ``n``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ActionType(str, Enum):
    """The four action kinds we materialise."""

    METADATA = "metadata"
    ADD = "add"
    REMOVE = "remove"
    COMMIT = "commit"


@dataclass(frozen=True, slots=True)
class FileEntry:
    """Reference to a data file living in the table directory."""

    path: str
    size_bytes: int
    record_count: int
    partition: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path must be non-empty")
        if self.size_bytes < 0:
            raise ValueError("size_bytes must be ≥ 0")
        if self.record_count < 0:
            raise ValueError("record_count must be ≥ 0")


@dataclass(frozen=True, slots=True)
class Action:
    """One action inside a commit entry."""

    type: ActionType
    file: FileEntry | None = None
    schema_id: int | None = None
    timestamp_ms: int | None = None
    commit_message: str | None = None

    def __post_init__(self) -> None:
        if self.type in (ActionType.ADD, ActionType.REMOVE) and self.file is None:
            raise ValueError(f"{self.type.value} action requires a file")
        if self.type is ActionType.METADATA and self.schema_id is None:
            raise ValueError("metadata action requires a schema_id")


__all__ = ["Action", "ActionType", "FileEntry"]
