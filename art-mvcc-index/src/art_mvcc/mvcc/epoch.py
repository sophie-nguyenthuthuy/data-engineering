"""Epoch-based reclamation (EBR).

Each reader thread enters/exits an epoch. The global epoch advances
periodically. Garbage retired at epoch X is safe to reclaim once no thread
is still in epoch ≤ X.

Semantics:
  - enter(tid)  — pin the current global epoch in the thread's record
  - leave(tid)  — clear the pin
  - retire(fn)  — fn() will run when it's safe to do so
  - gc()        — run any safe retirements
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class EpochManager:
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _epoch: int = 0
    _thread_epochs: dict[int, int] = field(default_factory=dict)
    _garbage: list[tuple[int, Callable[[], None]]] = field(default_factory=list)

    # ---- Public API -------------------------------------------------------

    def enter(self, tid: int | None = None) -> int:
        """Pin a thread to the current epoch. Returns the pinned epoch."""
        tid = tid if tid is not None else threading.get_ident()
        with self._lock:
            self._thread_epochs[tid] = self._epoch
            return self._epoch

    def leave(self, tid: int | None = None) -> None:
        tid = tid if tid is not None else threading.get_ident()
        with self._lock:
            self._thread_epochs.pop(tid, None)

    def advance(self) -> int:
        """Bump global epoch. Garbage retired in the new epoch will become
        safe once all threads currently in older epochs have left."""
        with self._lock:
            self._epoch += 1
            return self._epoch

    def retire(self, fn: Callable[[], None]) -> None:
        """Schedule `fn` for safe reclamation."""
        with self._lock:
            self._garbage.append((self._epoch, fn))

    def gc(self) -> int:
        """Run any retirements that are now safe. Returns count reclaimed."""
        with self._lock:
            if self._thread_epochs:
                threshold = min(self._thread_epochs.values())
            else:
                threshold = self._epoch + 1
            keep: list[tuple[int, Callable[[], None]]] = []
            reclaimed = 0
            for retire_epoch, fn in self._garbage:
                if retire_epoch < threshold:
                    import contextlib
                    with contextlib.suppress(Exception):  # pragma: no cover
                        fn()
                    reclaimed += 1
                else:
                    keep.append((retire_epoch, fn))
            self._garbage = keep
            return reclaimed

    # ---- Introspection ----------------------------------------------------

    @property
    def epoch(self) -> int:
        return self._epoch

    @property
    def active_threads(self) -> int:
        return len(self._thread_epochs)

    @property
    def pending_garbage(self) -> int:
        return len(self._garbage)

    # ---- Context manager --------------------------------------------------

    class _Guard:
        __slots__ = ("_entered_epoch", "mgr", "tid")

        def __init__(self, mgr: EpochManager, tid: int | None) -> None:
            self.mgr = mgr
            self.tid = tid
            self._entered_epoch = 0

        def __enter__(self) -> int:
            self._entered_epoch = self.mgr.enter(self.tid)
            return self._entered_epoch

        def __exit__(self, *exc: object) -> None:
            self.mgr.leave(self.tid)

    def guard(self, tid: int | None = None) -> EpochManager._Guard:
        return EpochManager._Guard(self, tid)


__all__ = ["EpochManager"]
