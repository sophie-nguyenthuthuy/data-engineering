"""Delta-Lake-shaped table.

Each commit writes one JSON log entry whose name is the
zero-padded next version number. Concurrent commits race on the same
filename: only one writer succeeds; the loser retries with the next
version. The whole table state is the *reduction* of every log entry
in order — the table is essentially an event-sourced object.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from tfl.delta.action import Action, ActionType, FileEntry


class DeltaConflict(RuntimeError):
    """Raised when two writers race for the same log version."""


def _log_filename(version: int) -> str:
    return f"_delta_log/{version:020d}.json"


@dataclass
class DeltaTable:
    """In-memory Delta-style transaction log + materialised state."""

    _entries: list[tuple[int, tuple[Action, ...]]] = field(default_factory=list, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    # ------------------------------------------------------------- write

    def commit(self, actions: list[Action], *, expected_version: int) -> int:
        """Append ``actions`` as version ``expected_version + 1``.

        Raises :class:`DeltaConflict` if another writer beat us to that
        version — exactly the Delta optimistic-concurrency contract.
        """
        if not actions:
            raise ValueError("commit must include at least one action")
        with self._lock:
            cur = self.version()
            if cur != expected_version:
                raise DeltaConflict(
                    f"commit expected version {expected_version} but table is at {cur}"
                )
            new_version = cur + 1
            self._entries.append((new_version, tuple(actions)))
            return new_version

    # ---------------------------------------------------------- read API

    def version(self) -> int:
        with self._lock:
            return self._entries[-1][0] if self._entries else -1

    def log_paths(self) -> list[str]:
        with self._lock:
            return [_log_filename(v) for v, _ in self._entries]

    def files_at(self, version: int | None = None) -> list[FileEntry]:
        """Replay the log up to ``version`` and return the live files."""
        target = self.version() if version is None else version
        with self._lock:
            live: dict[str, FileEntry] = {}
            for v, actions in self._entries:
                if v > target:
                    break
                for a in actions:
                    if a.type is ActionType.ADD and a.file is not None:
                        live[a.file.path] = a.file
                    elif a.type is ActionType.REMOVE and a.file is not None:
                        live.pop(a.file.path, None)
            return list(live.values())

    def current_schema_id(self) -> int | None:
        with self._lock:
            current: int | None = None
            for _, actions in self._entries:
                for a in actions:
                    if a.type is ActionType.METADATA:
                        current = a.schema_id
            return current

    # ---------------------------------------------------- maintenance

    def compact(self, files: list[FileEntry], replacement: FileEntry) -> int:
        """Rewrite ``files`` into a single ``replacement`` in one commit."""
        if not files:
            raise ValueError("compact requires ≥ 1 file to rewrite")
        actions = [Action(ActionType.REMOVE, file=f) for f in files]
        actions.append(Action(ActionType.ADD, file=replacement))
        return self.commit(actions, expected_version=self.version())

    def n_log_entries(self) -> int:
        with self._lock:
            return len(self._entries)


__all__ = ["DeltaConflict", "DeltaTable"]
