"""Schema management and evolution tracking.

A Schema is a snapshot of {column_name: dtype_str} for a table.
SchemaEvolutionTracker detects changes between successive schemas and
notifies an EncodingSelector so it can evict stale cache entries.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


class ChangeKind(Enum):
    ADDED = auto()
    REMOVED = auto()
    TYPE_CHANGED = auto()
    UNCHANGED = auto()


@dataclass
class ColumnChange:
    name: str
    kind: ChangeKind
    old_dtype: Optional[str]
    new_dtype: Optional[str]


@dataclass
class SchemaDiff:
    changes: list[ColumnChange] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return any(c.kind != ChangeKind.UNCHANGED for c in self.changes)

    @property
    def affected_columns(self) -> list[str]:
        return [c.name for c in self.changes if c.kind != ChangeKind.UNCHANGED]

    def __str__(self) -> str:
        lines = []
        for c in self.changes:
            if c.kind == ChangeKind.ADDED:
                lines.append(f"  + {c.name}: (new, {c.new_dtype})")
            elif c.kind == ChangeKind.REMOVED:
                lines.append(f"  - {c.name}: (removed)")
            elif c.kind == ChangeKind.TYPE_CHANGED:
                lines.append(f"  ~ {c.name}: {c.old_dtype} → {c.new_dtype}")
        return "\n".join(lines) if lines else "  (no changes)"


def schema_from_arrays(columns: dict[str, np.ndarray]) -> dict[str, str]:
    return {name: str(arr.dtype) for name, arr in columns.items()}


def diff_schemas(old: dict[str, str], new: dict[str, str]) -> SchemaDiff:
    changes: list[ColumnChange] = []
    all_names = set(old) | set(new)
    for name in sorted(all_names):
        old_dt = old.get(name)
        new_dt = new.get(name)
        if old_dt is None:
            changes.append(ColumnChange(name, ChangeKind.ADDED, None, new_dt))
        elif new_dt is None:
            changes.append(ColumnChange(name, ChangeKind.REMOVED, old_dt, None))
        elif old_dt != new_dt:
            changes.append(ColumnChange(name, ChangeKind.TYPE_CHANGED, old_dt, new_dt))
        else:
            changes.append(ColumnChange(name, ChangeKind.UNCHANGED, old_dt, new_dt))
    return SchemaDiff(changes)


class SchemaEvolutionTracker:
    """Track schema versions and propagate changes to an EncodingSelector."""

    def __init__(self) -> None:
        self._current: dict[str, str] = {}
        self._history: list[tuple[dict[str, str], SchemaDiff]] = []

    @property
    def current(self) -> dict[str, str]:
        return dict(self._current)

    def observe(self, columns: dict[str, np.ndarray], selector=None) -> SchemaDiff:
        """
        Register a new batch of columns.  If the schema changed, notify
        *selector* (an EncodingSelector) to evict stale codec choices.
        Returns the diff relative to the previous schema.
        """
        new_schema = schema_from_arrays(columns)
        diff = diff_schemas(self._current, new_schema)

        if diff.has_changes:
            logger.info("Schema evolution detected:\n%s", diff)
            if selector is not None:
                selector.schema_changed(self._current, new_schema)

        self._history.append((dict(self._current), diff))
        self._current = new_schema
        return diff

    def history(self) -> list[tuple[dict[str, str], SchemaDiff]]:
        return list(self._history)
