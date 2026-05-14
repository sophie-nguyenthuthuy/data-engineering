"""Per-tenant data-source registry.

A :class:`SourceSpec` is what a tenant registers through the
self-service portal: a logical name, a connector kind, and a free-form
config blob. Lookups are scoped to the tenant — one team cannot see
another's sources.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from typing import Any

_SOURCE_ID = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")


@dataclass(frozen=True, slots=True)
class SourceSpec:
    """One tenant-owned source definition."""

    tenant_id: str
    source_id: str
    kind: str
    config: dict[str, Any]

    def __post_init__(self) -> None:
        if not self.tenant_id:
            raise ValueError("tenant_id must be non-empty")
        if not _SOURCE_ID.match(self.source_id):
            raise ValueError(f"source_id {self.source_id!r} must match {_SOURCE_ID.pattern!r}")
        if not self.kind:
            raise ValueError("kind must be non-empty")


@dataclass
class SourceRegistry:
    """Tenant-scoped source registry."""

    _sources: dict[tuple[str, str], SourceSpec] = field(default_factory=dict, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def register(self, spec: SourceSpec) -> None:
        key = (spec.tenant_id, spec.source_id)
        with self._lock:
            if key in self._sources:
                raise ValueError(
                    f"source {spec.source_id!r} already registered for tenant {spec.tenant_id!r}"
                )
            self._sources[key] = spec

    def list_for(self, tenant_id: str) -> list[SourceSpec]:
        if not tenant_id:
            raise ValueError("tenant_id must be non-empty")
        with self._lock:
            return [s for (t, _), s in self._sources.items() if t == tenant_id]

    def get(self, tenant_id: str, source_id: str) -> SourceSpec:
        with self._lock:
            if (tenant_id, source_id) not in self._sources:
                raise KeyError(f"source {source_id!r} not registered for {tenant_id!r}")
            return self._sources[(tenant_id, source_id)]


__all__ = ["SourceRegistry", "SourceSpec"]
