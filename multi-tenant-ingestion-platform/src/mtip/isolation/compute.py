"""Per-tenant compute slot allocation.

A :class:`ComputeSlots` is the platform's worker pool, shared across
tenants. Each acquired slot is *tagged* with a tenant id so the
scheduler / admission controller can stop one tenant from soaking the
pool.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class ComputeSlots:
    """Bounded tagged-slot pool."""

    total: int
    _in_use: dict[str, int] = field(default_factory=dict, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def __post_init__(self) -> None:
        if self.total < 1:
            raise ValueError("total must be ≥ 1")

    def free(self) -> int:
        with self._lock:
            return self.total - sum(self._in_use.values())

    def in_use_for(self, tenant_id: str) -> int:
        with self._lock:
            return self._in_use.get(tenant_id, 0)

    def acquire(self, tenant_id: str, n: int = 1) -> bool:
        if n <= 0:
            raise ValueError("n must be > 0")
        if not tenant_id:
            raise ValueError("tenant_id must be non-empty")
        with self._lock:
            if self.total - sum(self._in_use.values()) < n:
                return False
            self._in_use[tenant_id] = self._in_use.get(tenant_id, 0) + n
            return True

    def release(self, tenant_id: str, n: int = 1) -> None:
        if n <= 0:
            raise ValueError("n must be > 0")
        with self._lock:
            cur = self._in_use.get(tenant_id, 0)
            new = max(0, cur - n)
            if new == 0:
                self._in_use.pop(tenant_id, None)
            else:
                self._in_use[tenant_id] = new


__all__ = ["ComputeSlots"]
