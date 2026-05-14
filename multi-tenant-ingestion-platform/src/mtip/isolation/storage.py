"""Per-tenant storage namespace.

We don't ship a real object-storage client; this module provides the
namespace-builder + path-validator every backend wires up to so no
tenant can read or write another tenant's data.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_TENANT_RE = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")


@dataclass(frozen=True, slots=True)
class StorageNamespace:
    """A tenant's storage root.

    The :meth:`resolve` method maps a relative path inside the tenant's
    namespace to its on-store key. Absolute paths and traversal segments
    are rejected.
    """

    tenant_id: str
    root_prefix: str = "tenants"

    def __post_init__(self) -> None:
        if not _TENANT_RE.match(self.tenant_id):
            raise ValueError(f"tenant_id {self.tenant_id!r} must match {_TENANT_RE.pattern!r}")
        if not self.root_prefix:
            raise ValueError("root_prefix must be non-empty")

    @property
    def base(self) -> str:
        return f"{self.root_prefix}/{self.tenant_id}"

    def resolve(self, relative_path: str) -> str:
        """Map a tenant-relative path to its fully-qualified storage key."""
        if not relative_path:
            raise ValueError("relative_path must be non-empty")
        if relative_path.startswith("/"):
            raise ValueError("relative_path must not be absolute")
        parts = [p for p in relative_path.split("/") if p]
        for p in parts:
            if p in {".", ".."}:
                raise ValueError("relative_path must not contain traversal segments")
        return f"{self.base}/{'/'.join(parts)}"


__all__ = ["StorageNamespace"]
