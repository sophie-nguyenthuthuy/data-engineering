"""Persistent Raft log with snapshotting support."""

import json
import os
import asyncio
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Any


@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]
    # For membership changes
    entry_type: str = "command"  # "command" | "config" | "noop"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LogEntry":
        return cls(**d)


@dataclass
class Snapshot:
    last_included_index: int
    last_included_term: int
    data: Dict[str, Any]  # serialized state machine state
    cluster_config: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Snapshot":
        return cls(**d)


class RaftLog:
    """
    Persistent log stored as line-delimited JSON.
    Supports snapshotting: once a snapshot is taken, entries before
    last_included_index are discarded.
    """

    def __init__(self, data_dir: str, node_id: str):
        self.data_dir = data_dir
        self.node_id = node_id
        os.makedirs(data_dir, exist_ok=True)

        self._log_path = os.path.join(data_dir, f"{node_id}.log")
        self._snapshot_path = os.path.join(data_dir, f"{node_id}.snapshot")
        self._state_path = os.path.join(data_dir, f"{node_id}.state")

        # In-memory log (entries after last snapshot)
        self._entries: List[LogEntry] = []

        # Snapshot metadata
        self.snapshot: Optional[Snapshot] = None

        # Persistent state (term + votedFor)
        self._current_term: int = 0
        self._voted_for: Optional[str] = None

        self._lock = asyncio.Lock()
        self._load()

    # ── Persistent state (currentTerm, votedFor) ──────────────────────────

    @property
    def current_term(self) -> int:
        return self._current_term

    @property
    def voted_for(self) -> Optional[str]:
        return self._voted_for

    async def save_term(self, term: int, voted_for: Optional[str]) -> None:
        async with self._lock:
            self._current_term = term
            self._voted_for = voted_for
            self._flush_state()

    # ── Log operations ─────────────────────────────────────────────────────

    @property
    def last_index(self) -> int:
        if self._entries:
            return self._entries[-1].index
        if self.snapshot:
            return self.snapshot.last_included_index
        return 0

    @property
    def last_term(self) -> int:
        if self._entries:
            return self._entries[-1].term
        if self.snapshot:
            return self.snapshot.last_included_term
        return 0

    def get_entry(self, index: int) -> Optional[LogEntry]:
        if self.snapshot and index <= self.snapshot.last_included_index:
            return None  # compacted
        offset = self._offset(index)
        if 0 <= offset < len(self._entries):
            return self._entries[offset]
        return None

    def get_term(self, index: int) -> int:
        if index == 0:
            return 0
        if self.snapshot and index == self.snapshot.last_included_index:
            return self.snapshot.last_included_term
        entry = self.get_entry(index)
        return entry.term if entry else 0

    def get_entries_from(self, start: int) -> List[LogEntry]:
        offset = self._offset(start)
        offset = max(offset, 0)
        return self._entries[offset:]

    async def append(self, entry: LogEntry) -> None:
        async with self._lock:
            self._entries.append(entry)
            self._flush_log_append(entry)

    async def append_entries(
        self,
        prev_log_index: int,
        prev_log_term: int,
        entries: List[LogEntry],
    ) -> bool:
        """
        Raft AppendEntries log consistency check + append.
        Returns False if the consistency check fails.
        """
        async with self._lock:
            # Check prev_log_index consistency
            if prev_log_index > 0:
                existing_term = self.get_term(prev_log_index)
                if existing_term != prev_log_term:
                    return False

            # Find conflict point and truncate
            for entry in entries:
                existing = self.get_entry(entry.index)
                if existing is not None and existing.term != entry.term:
                    # Conflict: truncate from here
                    offset = self._offset(entry.index)
                    self._entries = self._entries[:offset]
                    self._rewrite_log()

            # Append missing entries
            for entry in entries:
                if entry.index > self.last_index:
                    self._entries.append(entry)
                    self._flush_log_append(entry)

            return True

    async def take_snapshot(
        self,
        last_included_index: int,
        last_included_term: int,
        state: Dict[str, Any],
        config: List[str],
    ) -> None:
        async with self._lock:
            self.snapshot = Snapshot(
                last_included_index=last_included_index,
                last_included_term=last_included_term,
                data=state,
                cluster_config=config,
            )
            # Discard compacted entries
            offset = self._offset(last_included_index + 1)
            self._entries = self._entries[max(offset, 0):]
            self._flush_snapshot()
            self._rewrite_log()

    async def install_snapshot(self, snapshot: Snapshot) -> None:
        async with self._lock:
            if (
                self.snapshot is None
                or snapshot.last_included_index > self.snapshot.last_included_index
            ):
                self.snapshot = snapshot
                # Discard entries covered by snapshot
                offset = self._offset(snapshot.last_included_index + 1)
                if offset > 0:
                    self._entries = self._entries[max(offset, 0):]
                else:
                    self._entries = []
                self._flush_snapshot()
                self._rewrite_log()

    # ── Private helpers ────────────────────────────────────────────────────

    def _offset(self, index: int) -> int:
        base = self.snapshot.last_included_index if self.snapshot else 0
        return index - base - 1

    def _load(self) -> None:
        # Load snapshot
        if os.path.exists(self._snapshot_path):
            with open(self._snapshot_path) as f:
                self.snapshot = Snapshot.from_dict(json.load(f))

        # Load log entries
        if os.path.exists(self._log_path):
            with open(self._log_path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self._entries.append(LogEntry.from_dict(json.loads(line)))

        # Load persistent state
        if os.path.exists(self._state_path):
            with open(self._state_path) as f:
                s = json.load(f)
                self._current_term = s.get("current_term", 0)
                self._voted_for = s.get("voted_for")

    def _flush_log_append(self, entry: LogEntry) -> None:
        with open(self._log_path, "a") as f:
            f.write(json.dumps(entry.to_dict()) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _rewrite_log(self) -> None:
        with open(self._log_path, "w") as f:
            for e in self._entries:
                f.write(json.dumps(e.to_dict()) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _flush_snapshot(self) -> None:
        if self.snapshot:
            with open(self._snapshot_path, "w") as f:
                json.dump(self.snapshot.to_dict(), f)
                f.flush()
                os.fsync(f.fileno())

    def _flush_state(self) -> None:
        with open(self._state_path, "w") as f:
            json.dump(
                {"current_term": self._current_term, "voted_for": self._voted_for},
                f,
            )
            f.flush()
            os.fsync(f.fileno())
