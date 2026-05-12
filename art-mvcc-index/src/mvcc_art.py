"""ART + MVCC: integer-keyed index with snapshot-isolated reads."""
from __future__ import annotations

import struct
import threading
from dataclasses import dataclass

from .art import ART
from .mvcc import VersionChain, Version, EpochManager


def key_to_bytes(k: int) -> bytes:
    # 8-byte big-endian (preserves ordering)
    return struct.pack(">Q", k & 0xFFFFFFFFFFFFFFFF)


class MVCCArt:
    def __init__(self):
        self._art = ART()
        self._art_lock = threading.RLock()
        self._global_ts = 0
        self._ts_lock = threading.Lock()
        self.epoch_mgr = EpochManager()

    # ---- Timestamp ops ----------------------------------------------------

    def begin(self) -> "Snapshot":
        with self._ts_lock:
            ts = self._global_ts
        return Snapshot(self, ts)

    def commit_ts(self) -> int:
        with self._ts_lock:
            self._global_ts += 1
            return self._global_ts

    # ---- Writes (single-threaded simplification) --------------------------

    def put(self, key: int, value) -> int:
        ts = self.commit_ts()
        with self._art_lock:
            kb = key_to_bytes(key)
            chain: VersionChain | None = self._art.get(kb)
            if chain is None:
                chain = VersionChain()
                self._art.put(kb, chain)
            chain.append(Version(ts=ts, value=value, deleted=False))
        return ts

    def delete(self, key: int) -> int:
        ts = self.commit_ts()
        with self._art_lock:
            kb = key_to_bytes(key)
            chain: VersionChain | None = self._art.get(kb)
            if chain is None:
                chain = VersionChain()
                self._art.put(kb, chain)
            chain.append(Version(ts=ts, value=None, deleted=True))
        return ts

    # ---- Reads ------------------------------------------------------------

    def get_at(self, key: int, snapshot_ts: int):
        with self._art_lock:
            chain: VersionChain | None = self._art.get(key_to_bytes(key))
        if chain is None:
            return None
        v = chain.read(snapshot_ts)
        return v.value if v else None


@dataclass
class Snapshot:
    db: MVCCArt
    ts: int

    def get(self, key: int):
        return self.db.get_at(key, self.ts)

    def __enter__(self):
        tid = threading.get_ident()
        self.db.epoch_mgr.enter(tid)
        return self

    def __exit__(self, *exc):
        tid = threading.get_ident()
        self.db.epoch_mgr.leave(tid)


__all__ = ["MVCCArt", "Snapshot", "key_to_bytes"]
