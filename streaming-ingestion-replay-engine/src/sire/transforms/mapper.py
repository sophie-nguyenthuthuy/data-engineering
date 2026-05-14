"""Function-based mapper transform."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sire.transforms.base import Transform

if TYPE_CHECKING:
    from collections.abc import Callable

    from sire.log.record import Record


@dataclass
class Mapper(Transform):
    """Apply a ``Record -> Record`` callable."""

    fn: Callable[[Record], Record]

    def apply(self, record: Record) -> Record | object:
        out = self.fn(record)
        if not hasattr(out, "offset"):
            raise TypeError("Mapper.fn must return a Record-like object")
        return out


__all__ = ["Mapper"]
