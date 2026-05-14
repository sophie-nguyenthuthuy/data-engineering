"""Hudi-shaped tables.

Hudi indexes rows by a *record key* and stores them in
*file groups*. Two table types:

  * **CoW (Copy-on-Write)** — an UPDATE of any row in a file group
    rewrites the entire base file. Reads are cheap (one file per
    group) but writes amplify storage proportional to the file size.

  * **MoR (Merge-on-Read)** — an UPDATE appends a small "delta log"
    file inside the group. Reads have to merge base + log on the
    fly, but writes are tiny. A compaction job periodically merges
    deltas back into a new base file.

The timeline (``.hoodie/<commit_ts>.commit|.deltacommit|.compaction``)
is the version chain — semantically the same as Delta's
``_delta_log``, but each entry is tagged with the table-type-specific
operation.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable


class TimelineAction(str, Enum):
    """Action types we record on the timeline."""

    COMMIT = "commit"  # CoW: rewrote a base file
    DELTACOMMIT = "deltacommit"  # MoR: appended log files
    COMPACTION = "compaction"  # MoR: merged log + base → new base
    CLEAN = "clean"  # GC of obsolete files


@dataclass(frozen=True, slots=True)
class TimelineEntry:
    """One step on the Hudi timeline."""

    timestamp_ms: int
    action: TimelineAction
    file_group_id: str
    files_added: tuple[str, ...] = ()
    files_removed: tuple[str, ...] = ()


# --------------------------------------------------------- Copy-on-Write


@dataclass
class HudiCoWTable:
    """Copy-on-Write table.

    Each ``upsert`` rewrites the base file(s) for the affected file
    groups. Storage amplification is high under heavy updates, reads
    are cheap.
    """

    _base_files: dict[str, str] = field(default_factory=dict, repr=False)  # group → base path
    _timeline: list[TimelineEntry] = field(default_factory=list, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _clock_ms: int = field(default=0, repr=False)

    def upsert(self, group: str, new_base_path: str) -> int:
        if not group or not new_base_path:
            raise ValueError("group + new_base_path must be non-empty")
        with self._lock:
            self._clock_ms += 1
            removed: tuple[str, ...] = ()
            if group in self._base_files:
                removed = (self._base_files[group],)
            self._base_files[group] = new_base_path
            self._timeline.append(
                TimelineEntry(
                    timestamp_ms=self._clock_ms,
                    action=TimelineAction.COMMIT,
                    file_group_id=group,
                    files_added=(new_base_path,),
                    files_removed=removed,
                )
            )
            return self._clock_ms

    def read(self, group: str) -> str | None:
        with self._lock:
            return self._base_files.get(group)

    def files(self) -> list[str]:
        with self._lock:
            return sorted(self._base_files.values())

    def timeline(self) -> list[TimelineEntry]:
        with self._lock:
            return list(self._timeline)

    def write_amplification(self) -> int:
        """Total bytes-equivalent (we count files) written across all commits."""
        with self._lock:
            return sum(len(e.files_added) for e in self._timeline)


# --------------------------------------------------------- Merge-on-Read


@dataclass
class HudiMoRTable:
    """Merge-on-Read table.

    Writes append delta log files; reads merge base + logs; a
    compaction step periodically folds the logs back into a new base
    file.
    """

    _base_files: dict[str, str] = field(default_factory=dict, repr=False)
    _log_files: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list), repr=False)
    _timeline: list[TimelineEntry] = field(default_factory=list, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _clock_ms: int = field(default=0, repr=False)

    def insert_base(self, group: str, base_path: str) -> int:
        if not group or not base_path:
            raise ValueError("group + base_path must be non-empty")
        with self._lock:
            if group in self._base_files:
                raise ValueError(f"file group {group!r} already has a base file")
            self._clock_ms += 1
            self._base_files[group] = base_path
            self._timeline.append(
                TimelineEntry(
                    timestamp_ms=self._clock_ms,
                    action=TimelineAction.COMMIT,
                    file_group_id=group,
                    files_added=(base_path,),
                )
            )
            return self._clock_ms

    def append_log(self, group: str, log_path: str) -> int:
        if not group or not log_path:
            raise ValueError("group + log_path must be non-empty")
        with self._lock:
            if group not in self._base_files:
                raise ValueError(f"file group {group!r} has no base file")
            self._clock_ms += 1
            self._log_files[group].append(log_path)
            self._timeline.append(
                TimelineEntry(
                    timestamp_ms=self._clock_ms,
                    action=TimelineAction.DELTACOMMIT,
                    file_group_id=group,
                    files_added=(log_path,),
                )
            )
            return self._clock_ms

    def compact(self, group: str, new_base_path: str) -> int:
        if not group or not new_base_path:
            raise ValueError("group + new_base_path must be non-empty")
        with self._lock:
            if group not in self._base_files:
                raise ValueError(f"file group {group!r} has no base file")
            removed = (self._base_files[group], *self._log_files.pop(group, []))
            self._base_files[group] = new_base_path
            self._clock_ms += 1
            self._timeline.append(
                TimelineEntry(
                    timestamp_ms=self._clock_ms,
                    action=TimelineAction.COMPACTION,
                    file_group_id=group,
                    files_added=(new_base_path,),
                    files_removed=removed,
                )
            )
            return self._clock_ms

    def read_paths(self, group: str) -> tuple[str | None, tuple[str, ...]]:
        """Return ``(base_path, log_paths)`` — a merger combines them at read time."""
        with self._lock:
            return self._base_files.get(group), tuple(self._log_files.get(group, ()))

    def timeline(self) -> list[TimelineEntry]:
        with self._lock:
            return list(self._timeline)

    def write_amplification(self) -> int:
        with self._lock:
            return sum(len(e.files_added) for e in self._timeline)

    def all_groups(self) -> Iterable[str]:
        with self._lock:
            return tuple(self._base_files)


__all__ = ["HudiCoWTable", "HudiMoRTable", "TimelineAction", "TimelineEntry"]
