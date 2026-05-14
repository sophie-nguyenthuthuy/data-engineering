"""Transform base + the ``Skip`` sentinel."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sire.log.record import Record


SKIP = object()  # sentinel: filter said no, drop this record


class Transform(ABC):
    """A pure ``Record -> Record | SKIP`` mapping."""

    @abstractmethod
    def apply(self, record: Record) -> Record | object: ...


__all__ = ["SKIP", "Transform"]
