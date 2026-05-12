"""MVCC ART store: snapshot-isolated reads, transactional writes.

Architecture:
    MVCCArt
      ._index       : ART mapping key -> VersionChain
      ._next_ts     : monotone timestamp counter (per-store atomic)
      ._epoch       : EpochManager for safe version reclamation

A Snapshot captures (start_ts) at begin() and provides .get(key) for
isolated reads. Writes go through Transaction (see tx.py).
"""

from __future__ import annotations

import struct
import threading
from dataclasses import dataclass, field
from typing import Any

from art_mvcc.art.tree import ART
from art_mvcc.mvcc.epoch import EpochManager
from art_mvcc.mvcc.version import Version, VersionChain


def encode_int_key(k: int) -> bytes:
    """Big-endian encoding preserves natural key ordering."""
    return struct.pack(">q", k & 0xFFFFFFFFFFFFFFFF)


def decode_int_key(b: bytes) -> int:
    return struct.unpack(">q", b)[0]


class MVCCArt:
    """Multi-version-concurrency ART. Thread-safe for concurrent readers and
    serialised writers; uses snapshot isolation for reads.

    All keys are `bytes`. If you have integer keys, use `encode_int_key`.
    """

    def __init__(self) -> None:
        self._index = ART()
        self._index_lock = threading.RLock()    # protects ART structure
        self._ts_lock = threading.Lock()
        self._next_ts: int = 0
        self.epoch_mgr = EpochManager()

    # ---- Timestamp service ------------------------------------------------

    def now(self) -> int:
        """Read the current logical time (no advance)."""
        with self._ts_lock:
            return self._next_ts

    def tick(self) -> int:
        with self._ts_lock:
            self._next_ts += 1
            return self._next_ts

    # ---- Snapshots --------------------------------------------------------

    def begin_snapshot(self) -> Snapshot:
        return Snapshot(db=self, start_ts=self.now())

    # ---- Direct (auto-committing) writes ----------------------------------

    def put(self, key: bytes, value: Any) -> int:
        """Single-key auto-committed write. Returns the commit_ts."""
        ts = self.tick()
        with self._index_lock:
            chain = self._index.get(key)
            if chain is None:
                chain = VersionChain()
                self._index.put(key, chain)
        # Insert a committed version directly
        v = Version(commit_ts=ts, value=value, deleted=False, txn_id=None)
        chain._versions.insert(0, v)
        return ts

    def delete(self, key: bytes) -> int:
        ts = self.tick()
        with self._index_lock:
            chain = self._index.get(key)
            if chain is None:
                chain = VersionChain()
                self._index.put(key, chain)
        v = Version(commit_ts=ts, value=None, deleted=True, txn_id=None)
        chain._versions.insert(0, v)
        return ts

    def get_at(self, key: bytes, snapshot_ts: int) -> Any:
        with self._index_lock:
            chain = self._index.get(key)
        if chain is None:
            return None
        v = chain.read_at(snapshot_ts)
        return v.value if v else None

    def _get_or_create_chain(self, key: bytes) -> VersionChain:
        """Internal: caller must hold appropriate locks."""
        with self._index_lock:
            chain = self._index.get(key)
            if chain is None:
                chain = VersionChain()
                self._index.put(key, chain)
            return chain

    # ---- Garbage collection ----------------------------------------------

    def gc(self, watermark_ts: int | None = None) -> int:
        """Reclaim versions strictly older than `watermark_ts` (default: now-1).

        Returns total versions dropped.
        """
        ts = watermark_ts if watermark_ts is not None else self.now() - 1
        total = 0
        with self._index_lock:
            for _key, chain in self._index.items():
                total += chain.gc_below(ts)
        return total

    # ---- Diagnostics ------------------------------------------------------

    def size(self) -> int:
        return len(self._index)

    def chain(self, key: bytes) -> VersionChain | None:
        with self._index_lock:
            return self._index.get(key)


# ---------------------------------------------------------------------------
# Snapshot — isolated reader
# ---------------------------------------------------------------------------


@dataclass
class Snapshot:
    db: MVCCArt
    start_ts: int
    _epoch_token: Any = field(default=None, repr=False)

    def __enter__(self) -> Snapshot:
        self._epoch_token = self.db.epoch_mgr.guard()
        self._epoch_token.__enter__()
        return self

    def __exit__(self, *exc: object) -> None:
        if self._epoch_token is not None:
            self._epoch_token.__exit__(*exc)
            self._epoch_token = None

    def get(self, key: bytes) -> Any:
        return self.db.get_at(key, self.start_ts)

    def scan_prefix(self, prefix: bytes) -> list[tuple[bytes, Any]]:
        out: list[tuple[bytes, Any]] = []
        for k, chain in self.db._index.iter_prefix(prefix):
            v = chain.read_at(self.start_ts)
            if v is not None:
                out.append((k, v.value))
        return out


__all__ = ["MVCCArt", "Snapshot", "decode_int_key", "encode_int_key"]
