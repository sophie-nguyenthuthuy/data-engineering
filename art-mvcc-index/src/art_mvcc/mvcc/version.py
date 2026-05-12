"""Per-key version chains.

A version chain is a list of versions sorted by `commit_ts` descending.
Reads at snapshot ts find the *first* version with `commit_ts <= ts`.

Tombstones are encoded by `deleted=True`.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class Version:
    """One immutable version of a value."""
    commit_ts: int
    value: Any
    deleted: bool = False
    # txn_id is set during write before commit; cleared / replaced by commit_ts
    # at commit time. None means "committed".
    txn_id: int | None = None


class VersionChain:
    """Thread-safe per-key chain.

    Newest-first internally. Append-only at the head except for in-place
    `commit()` which finalises a previously written tentative version.
    """

    __slots__ = ("_lock", "_versions")

    def __init__(self) -> None:
        # versions are stored in descending commit_ts order; uncommitted
        # versions (txn_id != None) are pinned at the head until commit.
        self._versions: list[Version] = []
        self._lock = threading.RLock()

    def read_at(self, snapshot_ts: int) -> Version | None:
        """Snapshot read: latest version with commit_ts <= snapshot_ts AND committed."""
        with self._lock:
            for v in self._versions:
                if v.txn_id is not None:
                    continue  # skip uncommitted
                if v.commit_ts <= snapshot_ts:
                    return None if v.deleted else v
            return None

    def latest_committed(self) -> Version | None:
        with self._lock:
            for v in self._versions:
                if v.txn_id is None:
                    return v
        return None

    def tentative_write(self, txn_id: int, value: Any, deleted: bool = False) -> Version:
        """Append an uncommitted version. Caller must ensure no other tentative
        exists (use `has_uncommitted_after_ts` first)."""
        with self._lock:
            v = Version(commit_ts=-1, value=value, deleted=deleted, txn_id=txn_id)
            self._versions.insert(0, v)
            return v

    def has_committed_after_ts(self, ts: int) -> bool:
        """True if any committed version has commit_ts > ts. Used by snapshot
        isolation's first-committer-wins check."""
        with self._lock:
            for v in self._versions:
                if v.txn_id is None and v.commit_ts > ts:
                    return True
                # Once we pass uncommitted entries, list is descending so we
                # can early-exit on the first committed with commit_ts <= ts.
                if v.txn_id is None and v.commit_ts <= ts:
                    return False
            return False

    def has_uncommitted_other(self, txn_id: int) -> bool:
        """True if some *other* transaction has an uncommitted version."""
        with self._lock:
            return any(v.txn_id is not None and v.txn_id != txn_id
                       for v in self._versions)

    def commit(self, txn_id: int, commit_ts: int) -> None:
        """Finalise the tentative version owned by `txn_id`."""
        with self._lock:
            for v in self._versions:
                if v.txn_id == txn_id:
                    v.txn_id = None
                    v.commit_ts = commit_ts
                    # Re-sort: tentative was at head; ensure descending order
                    self._versions.sort(key=lambda x: (x.txn_id is None, -x.commit_ts),
                                        reverse=False)
                    # Actually we want committed in descending commit_ts;
                    # uncommitted should remain at head with txn_id != None.
                    # Re-do correctly:
                    break
            self._versions = sorted(self._versions,
                                    key=lambda x: (
                                        0 if x.txn_id is not None else 1,
                                        # uncommitted first (sort key 0)
                                        # then committed by -commit_ts ascending
                                        -x.commit_ts if x.txn_id is None else 0,
                                    ))

    def rollback(self, txn_id: int) -> None:
        """Drop the tentative version owned by `txn_id`."""
        with self._lock:
            self._versions = [v for v in self._versions if v.txn_id != txn_id]

    def gc_below(self, ts: int) -> int:
        """Drop versions with commit_ts strictly less than the oldest reachable
        committed version visible at snapshot `ts`. Returns count dropped.

        We keep the most-recent version with commit_ts <= ts as the "base"
        visible to old snapshots, plus everything newer.
        """
        with self._lock:
            # Find first committed version with commit_ts <= ts
            base_idx: int | None = None
            for i, v in enumerate(self._versions):
                if v.txn_id is None and v.commit_ts <= ts:
                    base_idx = i
                    break
            if base_idx is None:
                return 0
            n_before = len(self._versions)
            self._versions = self._versions[: base_idx + 1]
            return n_before - len(self._versions)

    def __len__(self) -> int:
        return len(self._versions)

    def snapshot(self) -> list[Version]:
        with self._lock:
            return list(self._versions)


__all__ = ["Version", "VersionChain"]
