"""Sharded coherence directory (write-invalidate protocol).

Each page has a directory entry tracking:
  - the set of client_ids that currently have a valid local copy (`holders`)
  - the exclusive writer (`writer`), or None if no write in flight

On read: client is added to `holders`.
On write: every other holder is told to invalidate; the writer becomes the
sole holder.

Sharding: directory is split into N shards keyed by page-id hash, each with
its own lock. This reduces contention vs. a single global directory lock.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from disagg.core.page import PageId


@dataclass(slots=True)
class CoherenceState:
    holders: set[int] = field(default_factory=set)
    writer: int | None = None
    # `version` here mirrors the page's version — used to detect stale reads
    version: int = 0


class _DirShard:
    """One shard's state + lock."""

    __slots__ = ("entries", "lock")

    def __init__(self) -> None:
        self.entries: dict[PageId, CoherenceState] = {}
        self.lock = threading.Lock()


class CoherenceDirectory:
    """Sharded, thread-safe directory."""

    def __init__(self, n_shards: int = 16) -> None:
        self._shards: list[_DirShard] = [_DirShard() for _ in range(n_shards)]

    def _shard(self, page_id: PageId) -> _DirShard:
        return self._shards[hash(page_id) % len(self._shards)]

    def register_reader(self, page_id: PageId, client_id: int, version: int) -> None:
        sh = self._shard(page_id)
        with sh.lock:
            entry = sh.entries.setdefault(page_id, CoherenceState())
            entry.holders.add(client_id)
            entry.version = max(entry.version, version)

    def register_writer(
        self, page_id: PageId, client_id: int, new_version: int
    ) -> list[int]:
        """Returns the list of OTHER clients that should invalidate."""
        sh = self._shard(page_id)
        with sh.lock:
            entry = sh.entries.setdefault(page_id, CoherenceState())
            others = list(entry.holders - {client_id})
            entry.holders = {client_id}
            entry.writer = client_id
            entry.version = new_version
            return others

    def release(self, page_id: PageId, client_id: int) -> None:
        sh = self._shard(page_id)
        with sh.lock:
            entry = sh.entries.get(page_id)
            if entry is None:
                return
            entry.holders.discard(client_id)
            if entry.writer == client_id:
                entry.writer = None

    def state(self, page_id: PageId) -> CoherenceState | None:
        sh = self._shard(page_id)
        with sh.lock:
            entry = sh.entries.get(page_id)
            if entry is None:
                return None
            return CoherenceState(
                holders=set(entry.holders),
                writer=entry.writer,
                version=entry.version,
            )

    def all_pages(self) -> Iterable[PageId]:
        for sh in self._shards:
            with sh.lock:
                yield from list(sh.entries.keys())

    def shard_count(self) -> int:
        return len(self._shards)
