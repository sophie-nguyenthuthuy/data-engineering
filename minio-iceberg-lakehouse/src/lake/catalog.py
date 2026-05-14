"""In-memory catalog — Hive-like (name → metadata location) registry."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


class CatalogError(RuntimeError):
    """Raised when the catalog cannot complete a lookup."""


@dataclass
class Catalog:
    """Thread-safe (namespace, name) → metadata-pointer registry."""

    _tables: dict[tuple[str, str], str] = field(default_factory=dict, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def register(self, *, namespace: str, name: str, metadata_path: str) -> None:
        if not namespace or not name:
            raise ValueError("namespace and name must be non-empty")
        if not metadata_path:
            raise ValueError("metadata_path must be non-empty")
        with self._lock:
            if (namespace, name) in self._tables:
                raise CatalogError(f"table {namespace}.{name} already registered")
            self._tables[(namespace, name)] = metadata_path

    def update_pointer(self, *, namespace: str, name: str, metadata_path: str) -> None:
        with self._lock:
            if (namespace, name) not in self._tables:
                raise CatalogError(f"unknown table {namespace}.{name}")
            self._tables[(namespace, name)] = metadata_path

    def lookup(self, namespace: str, name: str) -> str:
        with self._lock:
            if (namespace, name) not in self._tables:
                raise CatalogError(f"unknown table {namespace}.{name}")
            return self._tables[(namespace, name)]

    def list_tables(self, namespace: str) -> list[str]:
        with self._lock:
            return sorted(n for (ns, n) in self._tables if ns == namespace)

    def list_namespaces(self) -> list[str]:
        with self._lock:
            return sorted({ns for ns, _ in self._tables})

    def drop(self, namespace: str, name: str) -> None:
        with self._lock:
            if (namespace, name) not in self._tables:
                raise CatalogError(f"unknown table {namespace}.{name}")
            del self._tables[(namespace, name)]


__all__ = ["Catalog", "CatalogError"]
