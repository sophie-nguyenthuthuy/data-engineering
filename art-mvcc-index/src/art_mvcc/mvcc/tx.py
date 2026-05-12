"""Transactions: snapshot-isolated reads, first-committer-wins write conflicts.

Lifecycle:
    txn = db.begin_tx()
    txn.put(k, v)        # tentative
    txn.delete(k)
    val = txn.get(k)     # reads from snapshot + own tentative writes
    txn.commit()         # may raise TxConflict
    # or
    txn.rollback()

Semantics:
    - Start: take a snapshot ts0.
    - Reads: see committed versions with commit_ts <= ts0, plus this txn's
      own tentative writes.
    - Writes: append a tentative version to the chain. If another committed
      version exists with commit_ts > ts0, this txn must abort
      (first-committer-wins).
    - Commit: assign a fresh commit_ts > all start_ts; finalise tentatives.
"""

from __future__ import annotations

import itertools
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from art_mvcc.mvcc.store import MVCCArt


_TXN_ID_COUNTER = itertools.count(1)
_TXN_ID_LOCK = threading.Lock()


class TxConflict(Exception):
    """Raised when commit detects a write-write conflict."""


class Transaction:
    __slots__ = ("_aborted", "_committed", "_writes", "db", "start_ts", "txn_id")

    def __init__(self, db: MVCCArt) -> None:
        self.db = db
        self.start_ts = db.now()
        with _TXN_ID_LOCK:
            self.txn_id = next(_TXN_ID_COUNTER)
        self._writes: dict[bytes, tuple[Any, bool]] = {}   # key -> (value, deleted)
        self._committed = False
        self._aborted = False

    # ---- Context manager --------------------------------------------------

    def __enter__(self) -> Transaction:
        return self

    def __exit__(self, exc_type: object, *_: object) -> None:
        if exc_type is not None and not (self._committed or self._aborted):
            self.rollback()
        # Auto-commit not done — caller must explicitly commit.

    # ---- Reads ------------------------------------------------------------

    def get(self, key: bytes) -> Any:
        self._ensure_active()
        # Own writes shadow snapshot
        if key in self._writes:
            value, deleted = self._writes[key]
            return None if deleted else value
        return self.db.get_at(key, self.start_ts)

    # ---- Writes -----------------------------------------------------------

    def put(self, key: bytes, value: Any) -> None:
        self._ensure_active()
        self._writes[key] = (value, False)

    def delete(self, key: bytes) -> None:
        self._ensure_active()
        self._writes[key] = (None, True)

    # ---- Commit / Rollback ------------------------------------------------

    def commit(self) -> int:
        self._ensure_active()

        # First pass: place tentatives, check for conflicts. We hold each
        # chain's lock per-key to detect write-write conflicts atomically.
        prepared: list = []
        try:
            for key, (value, deleted) in self._writes.items():
                chain = self.db._get_or_create_chain(key)
                with chain._lock:
                    # Conflict: another txn already committed AFTER our start
                    if chain.has_committed_after_ts(self.start_ts):
                        raise TxConflict(
                            f"write-write conflict on {key!r} "
                            f"(our start_ts={self.start_ts})"
                        )
                    # Conflict: another txn's tentative pending
                    if chain.has_uncommitted_other(self.txn_id):
                        raise TxConflict(
                            f"concurrent tentative write on {key!r}"
                        )
                    chain.tentative_write(self.txn_id, value, deleted)
                    prepared.append(chain)
        except TxConflict:
            for chain in prepared:
                chain.rollback(self.txn_id)
            self._aborted = True
            raise

        # Second pass: assign commit_ts, finalise.
        commit_ts = self.db.tick()
        for chain in prepared:
            chain.commit(self.txn_id, commit_ts)
        self._committed = True
        return commit_ts

    def rollback(self) -> None:
        if self._committed or self._aborted:
            return
        self._aborted = True
        # Best-effort: drop tentatives if any were placed (only commit() places)
        for key in self._writes:
            chain = self.db.chain(key)
            if chain is not None:
                chain.rollback(self.txn_id)

    # ---- Internals --------------------------------------------------------

    def _ensure_active(self) -> None:
        if self._committed:
            raise RuntimeError("transaction already committed")
        if self._aborted:
            raise RuntimeError("transaction aborted")


def begin_tx(db: MVCCArt) -> Transaction:
    return Transaction(db)


__all__ = ["Transaction", "TxConflict", "begin_tx"]
