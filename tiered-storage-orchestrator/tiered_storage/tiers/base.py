from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

from tiered_storage.schemas import DataRecord, TierMetrics


class BaseTier(ABC):
    """Abstract interface every tier must implement."""

    @abstractmethod
    async def get(self, key: str) -> Optional[DataRecord]:
        """Retrieve a record; return None on miss."""

    @abstractmethod
    async def put(self, record: DataRecord) -> None:
        """Store or overwrite a record."""

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Remove a record. Returns True if it existed."""

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Return True if the key exists in this tier."""

    @abstractmethod
    async def metrics(self) -> TierMetrics:
        """Return current tier metrics."""

    @abstractmethod
    async def list_keys(
        self, prefix: str = "", limit: int = 1000
    ) -> list[str]:
        """List keys, optionally filtered by prefix."""

    async def close(self) -> None:
        """Tear down connections gracefully."""
