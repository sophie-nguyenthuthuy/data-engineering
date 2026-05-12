"""Multi-version concurrency control over ART, with epoch-based reclamation.

Each leaf entry stores a chain of versions {(ts, value)}. A transaction reads
at its snapshot ts; the resolver returns the version with the largest ts ≤
snapshot_ts.

Writes append to the chain. Old versions become unreachable when no snapshot
with ts ≤ their ts is still active — reclamation is handled in epochs.
"""
from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Version:
    ts: int
    value: object
    deleted: bool = False


@dataclass
class VersionChain:
    """Newest-first list of versions."""
    versions: list = field(default_factory=list)

    def append(self, v: Version) -> None:
        # Keep sorted by ts descending
        self.versions.append(v)
        self.versions.sort(key=lambda x: -x.ts)

    def read(self, snapshot_ts: int) -> Optional[Version]:
        for v in self.versions:
            if v.ts <= snapshot_ts:
                return None if v.deleted else v
        return None

    def reclaim_below(self, ts: int) -> int:
        """Drop versions strictly older than the oldest reachable. Returns count dropped."""
        n_before = len(self.versions)
        # Keep all versions that any active snapshot ts >= might need.
        # Conservative: keep one version with ts <= cutoff (the visible base)
        # and all versions with ts > cutoff.
        if not self.versions:
            return 0
        # Find the largest ts ≤ ts among versions (the "base" version visible at ts)
        base_idx = None
        for i, v in enumerate(self.versions):
            if v.ts <= ts:
                base_idx = i
                break
        if base_idx is None:
            return 0  # all versions > ts; nothing to reclaim
        # Drop everything *after* base_idx (which are older)
        kept = self.versions[: base_idx + 1]
        self.versions = kept
        return n_before - len(self.versions)


class EpochManager:
    """Each thread publishes a monotone epoch when starting a critical section.
    Garbage with retire-epoch < min(active epochs) is safe to reclaim."""

    def __init__(self):
        self._lock = threading.Lock()
        self._epoch = 0
        self._thread_epochs: dict = {}     # tid → epoch at last entry
        self._garbage: list = []           # list[(retire_epoch, callable)]

    def enter(self, tid):
        with self._lock:
            self._thread_epochs[tid] = self._epoch

    def leave(self, tid):
        with self._lock:
            self._thread_epochs.pop(tid, None)

    def advance(self) -> int:
        with self._lock:
            self._epoch += 1
            return self._epoch

    def retire(self, retire_epoch: int, fn) -> None:
        with self._lock:
            self._garbage.append((retire_epoch, fn))

    def gc(self) -> int:
        """Reclaim safe garbage.

        Semantics:
          - If at least one thread is active at epoch X, garbage retired at
            epoch >= X is NOT safe (the thread might hold a pre-retire pointer).
            → threshold = min_active
          - If no thread is active, ALL retired garbage is safe.
            → threshold = epoch + 1
        """
        with self._lock:
            if self._thread_epochs:
                threshold = min(self._thread_epochs.values())
            else:
                threshold = self._epoch + 1
            keep = []
            reclaimed = 0
            for retire_epoch, fn in self._garbage:
                if retire_epoch < threshold:
                    fn()
                    reclaimed += 1
                else:
                    keep.append((retire_epoch, fn))
            self._garbage = keep
            return reclaimed

    @property
    def epoch(self) -> int:
        return self._epoch


__all__ = ["Version", "VersionChain", "EpochManager"]
