"""Composed transform — apply a chain of :class:`Transform` in order."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sire.transforms.base import SKIP, Transform

if TYPE_CHECKING:
    from sire.log.record import Record


@dataclass
class ComposedTransform(Transform):
    """Apply ``transforms[0] → transforms[1] → …`` short-circuiting on SKIP."""

    transforms: list[Transform] = field(default_factory=list)

    def apply(self, record: Record) -> Record | object:
        cur: Record | object = record
        for t in self.transforms:
            cur = t.apply(cur)  # type: ignore[arg-type]
            if cur is SKIP:
                return SKIP
        return cur


__all__ = ["ComposedTransform"]
