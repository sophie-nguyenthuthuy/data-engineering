"""Thread-safe in-memory storage backend.

Production deployments back the bucket with Redis + Lua; this backend
is the deterministic test double. The atomicity primitive is a
``threading.RLock`` — every read-modify-write is serialised.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from arlo.storage.base import BucketState, StorageBackend


@dataclass
class InMemoryStorage(StorageBackend):
    """RLock-guarded in-memory state, one entry per bucket key."""

    _state: dict[str, BucketState] = field(default_factory=dict, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def atomic_take(
        self,
        key: str,
        *,
        capacity: float,
        refill_per_second: float,
        requested: float,
        now: float,
    ) -> tuple[bool, BucketState]:
        if requested < 0:
            raise ValueError("requested must be ≥ 0")
        with self._lock:
            cur = self._state.get(key)
            if cur is None:
                # First touch — start full at the current timestamp.
                tokens = capacity
                last = now
            else:
                elapsed = max(0.0, now - cur.last_refill_ts)
                tokens = min(capacity, cur.tokens + elapsed * refill_per_second)
                last = now
            if requested <= tokens:
                new = BucketState(tokens=tokens - requested, last_refill_ts=last)
                self._state[key] = new
                return True, new
            new = BucketState(tokens=tokens, last_refill_ts=last)
            self._state[key] = new
            return False, new


__all__ = ["InMemoryStorage"]
