"""Predicate-based filter transform."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sire.transforms.base import SKIP, Transform

if TYPE_CHECKING:
    from collections.abc import Callable

    from sire.log.record import Record


@dataclass
class Filter(Transform):
    """Drop records where ``predicate(record)`` is ``False``."""

    predicate: Callable[[Record], bool]

    def apply(self, record: Record) -> Record | object:
        return record if self.predicate(record) else SKIP


__all__ = ["Filter"]
