"""Transform base class.

A :class:`Transform` is a pure ``DebeziumEnvelope → DebeziumEnvelope``
mapping. Composability is delegated to :class:`Pipeline` in
``cdc.pipeline`` so individual transforms stay simple.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cdc.events.envelope import DebeziumEnvelope


class Transform(ABC):
    """ABC for stateless event-level transforms."""

    name: str = "transform"

    @abstractmethod
    def apply(self, envelope: DebeziumEnvelope) -> DebeziumEnvelope: ...


__all__ = ["Transform"]
