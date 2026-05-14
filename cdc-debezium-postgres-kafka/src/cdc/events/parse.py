"""Debezium-JSON → :class:`DebeziumEnvelope` parser.

Accepts either the raw JSON bytes or an already-decoded ``dict``. Any
deviation from the documented shape raises :class:`ParseError` with a
breadcrumb of the path that failed (``source.db``, ``after``, …) so
DLQ routing can pin the failure on a specific event.
"""

from __future__ import annotations

import json
from typing import Any

from cdc.events.envelope import DebeziumEnvelope, Op, SourceInfo


class ParseError(ValueError):
    """Raised when a Debezium payload is malformed."""


def parse_envelope(payload: bytes | str | dict[str, Any]) -> DebeziumEnvelope:
    """Decode and validate one Debezium event."""
    if isinstance(payload, bytes | str):
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ParseError(f"invalid JSON: {exc}") from exc
    else:
        obj = payload

    if not isinstance(obj, dict):
        raise ParseError(f"event must be a JSON object, got {type(obj).__name__}")

    op_raw = _require(obj, "op", str)
    try:
        op = Op.parse(op_raw)
    except ValueError as exc:
        raise ParseError(str(exc)) from exc

    source_obj = _require(obj, "source", dict)
    try:
        source = SourceInfo(
            db=_require(source_obj, "db", str),
            schema=_require(source_obj, "schema", str),
            table=_require(source_obj, "table", str),
            ts_ms=_require(source_obj, "ts_ms", int),
            lsn=source_obj.get("lsn"),
            txid=source_obj.get("txid"),
        )
    except (TypeError, ValueError) as exc:
        raise ParseError(f"bad source block: {exc}") from exc

    ts_ms = _require(obj, "ts_ms", int)

    before = _typed_or_none(obj, "before", dict)
    after = _typed_or_none(obj, "after", dict)

    try:
        return DebeziumEnvelope(
            op=op,
            source=source,
            ts_ms=ts_ms,
            before=before,
            after=after,
        )
    except ValueError as exc:
        raise ParseError(str(exc)) from exc


def _require(obj: dict[str, Any], key: str, kind: type) -> Any:
    if key not in obj:
        raise ParseError(f"missing required key {key!r}")
    val = obj[key]
    if not isinstance(val, kind):
        raise ParseError(f"key {key!r} expected {kind.__name__} but got {type(val).__name__}")
    return val


def _typed_or_none(obj: dict[str, Any], key: str, kind: type) -> Any:
    val = obj.get(key)
    if val is None:
        return None
    if not isinstance(val, kind):
        raise ParseError(
            f"key {key!r} expected {kind.__name__} or null but got {type(val).__name__}"
        )
    return val


__all__ = ["ParseError", "parse_envelope"]
