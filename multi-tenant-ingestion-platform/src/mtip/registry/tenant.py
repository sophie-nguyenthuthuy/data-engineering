"""Tenant model + registry."""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mtip.quota import ResourceQuota


_TENANT_ID = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")


@dataclass(frozen=True, slots=True)
class Tenant:
    """A registered tenant + their resource quota."""

    id: str
    display_name: str
    quota: ResourceQuota

    def __post_init__(self) -> None:
        if not _TENANT_ID.match(self.id):
            raise ValueError(f"tenant id {self.id!r} must match {_TENANT_ID.pattern!r}")
        if not self.display_name:
            raise ValueError("display_name must be non-empty")


@dataclass
class TenantRegistry:
    """Thread-safe tenant registry."""

    _tenants: dict[str, Tenant] = field(default_factory=dict, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def register(self, tenant: Tenant) -> None:
        with self._lock:
            if tenant.id in self._tenants:
                raise ValueError(f"tenant {tenant.id!r} already registered")
            self._tenants[tenant.id] = tenant

    def get(self, tenant_id: str) -> Tenant:
        with self._lock:
            if tenant_id not in self._tenants:
                raise KeyError(f"unknown tenant {tenant_id!r}")
            return self._tenants[tenant_id]

    def all(self) -> list[Tenant]:
        with self._lock:
            return list(self._tenants.values())

    def __contains__(self, tenant_id: str) -> bool:
        with self._lock:
            return tenant_id in self._tenants

    def __len__(self) -> int:
        with self._lock:
            return len(self._tenants)


__all__ = ["Tenant", "TenantRegistry"]
