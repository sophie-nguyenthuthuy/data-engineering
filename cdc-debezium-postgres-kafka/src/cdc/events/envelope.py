"""Debezium event envelope.

A Debezium change event has a stable JSON envelope:

    {
      "op": "c" | "u" | "d" | "r",
      "before": {column: value, ...} | null,
      "after":  {column: value, ...} | null,
      "source": { "db": ..., "schema": ..., "table": ..., "ts_ms": ..., ... },
      "ts_ms":  <ms-since-epoch>
    }

We expose this as a frozen :class:`DebeziumEnvelope` whose constructor
asserts the standard invariants for every ``op`` (a "create" event must
have ``after``, a "delete" must have ``before``, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Op(str, Enum):
    """Debezium operation codes."""

    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"  # initial-snapshot / read event

    @classmethod
    def parse(cls, raw: str) -> Op:
        try:
            return cls(raw)
        except ValueError as exc:
            raise ValueError(f"unknown Debezium op {raw!r}") from exc


@dataclass(frozen=True, slots=True)
class SourceInfo:
    """The ``source`` block of a Debezium envelope."""

    db: str
    schema: str
    table: str
    ts_ms: int
    lsn: int | None = None
    txid: int | None = None

    def __post_init__(self) -> None:
        if not self.db or not self.schema or not self.table:
            raise ValueError("source.db/schema/table must all be non-empty")
        if self.ts_ms < 0:
            raise ValueError("source.ts_ms must be ≥ 0")


@dataclass(frozen=True, slots=True)
class DebeziumEnvelope:
    """Parsed CDC event with shape invariants enforced."""

    op: Op
    source: SourceInfo
    ts_ms: int
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.ts_ms < 0:
            raise ValueError("ts_ms must be ≥ 0")
        if self.op in (Op.CREATE, Op.READ) and self.after is None:
            raise ValueError(f"op={self.op.value!r} requires non-null 'after'")
        if self.op == Op.DELETE and self.before is None:
            raise ValueError("op='d' requires non-null 'before'")
        if self.op == Op.UPDATE and (self.before is None or self.after is None):
            raise ValueError("op='u' requires both 'before' and 'after'")

    def primary_key(self, key_columns: tuple[str, ...]) -> tuple[Any, ...]:
        """Pluck the primary-key tuple from the post-state (or pre-state for deletes)."""
        if not key_columns:
            raise ValueError("key_columns must be non-empty")
        row = self.after if self.after is not None else self.before
        if row is None:
            raise ValueError("envelope has neither before nor after")
        try:
            return tuple(row[c] for c in key_columns)
        except KeyError as exc:
            raise ValueError(f"key column missing in row: {exc}") from exc


__all__ = ["DebeziumEnvelope", "Op", "SourceInfo"]
