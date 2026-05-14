"""Backend protocol."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from ifw.events import FileEvent


class Backend(ABC):
    """Iterator-of-events over the watched object store."""

    kind: str = "abstract"

    @abstractmethod
    def poll(self) -> Iterator[FileEvent]:
        """Yield events available *right now* and return."""


__all__ = ["Backend"]
