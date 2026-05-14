"""Rename columns in before/after rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from cdc.transforms.base import Transform

if TYPE_CHECKING:
    from cdc.events.envelope import DebeziumEnvelope


@dataclass
class RenameColumns(Transform):
    """Rename each ``old_name → new_name`` pair in both before and after."""

    mapping: dict[str, str]
    name: str = "rename"

    def __post_init__(self) -> None:
        if not self.mapping:
            raise ValueError("mapping must be non-empty")
        if len(set(self.mapping.values())) != len(self.mapping):
            raise ValueError("duplicate destination column name in mapping")

    def apply(self, envelope: DebeziumEnvelope) -> DebeziumEnvelope:
        from cdc.events.envelope import DebeziumEnvelope as _Env

        return _Env(
            op=envelope.op,
            source=envelope.source,
            ts_ms=envelope.ts_ms,
            before=self._rename(envelope.before),
            after=self._rename(envelope.after),
            extra=envelope.extra,
        )

    def _rename(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {self.mapping.get(k, k): v for k, v in row.items()}


__all__ = ["RenameColumns"]
