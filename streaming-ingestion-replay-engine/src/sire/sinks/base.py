"""Sink protocol."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sire.log.record import Record


class Sink(ABC):
    """Receives transformed records from the replay engine."""

    @abstractmethod
    def write(self, record: Record) -> None: ...

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


__all__ = ["Sink"]
