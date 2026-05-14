"""Flatten ``after`` (or ``before`` on a delete) into a top-level row.

The output envelope's ``extra["row"]`` carries the post-change row;
downstream sinks that don't care about Debezium semantics consume that
directly.
"""

from __future__ import annotations

from dataclasses import dataclass

from cdc.events.envelope import DebeziumEnvelope, Op
from cdc.transforms.base import Transform


@dataclass
class FlattenAfter(Transform):
    """Pin ``extra['row']`` to ``after`` (or ``before`` for deletes)."""

    name: str = "flatten_after"

    def apply(self, envelope: DebeziumEnvelope) -> DebeziumEnvelope:
        row = envelope.after if envelope.op != Op.DELETE else envelope.before
        if row is None:
            raise ValueError(f"flatten: op={envelope.op.value!r} has no usable row")
        new_extra = dict(envelope.extra)
        new_extra["row"] = dict(row)
        return DebeziumEnvelope(
            op=envelope.op,
            source=envelope.source,
            ts_ms=envelope.ts_ms,
            before=envelope.before,
            after=envelope.after,
            extra=new_extra,
        )


__all__ = ["FlattenAfter"]
