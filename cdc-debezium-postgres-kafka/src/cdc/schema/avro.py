"""Avro schema generation for Debezium events.

Two helpers:

  * :func:`postgres_to_avro` — Postgres type-name → Avro type (or
    list-with-null union for nullable columns).
  * :func:`generate_avro_schema` — given a fully-qualified table name
    and a list of ``(column, postgres_type, nullable)`` triples, emit
    a Schema-Registry-compatible record schema for the **flattened**
    after row.

The mapping is intentionally small and explicit. Anything we don't
recognise is left as ``"string"`` with a ``logicalType`` of ``unknown``
so the resulting schema is at least valid Avro and downstream
consumers can fall back to JSON parsing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

# Conservative Postgres → Avro mapping. We default unknown columns to
# string so the generator never produces invalid Avro.
_POSTGRES_TO_AVRO: dict[str, Any] = {
    "smallint": "int",
    "integer": "int",
    "int": "int",
    "int4": "int",
    "int8": "long",
    "bigint": "long",
    "real": "float",
    "double": "double",
    "double precision": "double",
    "numeric": {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9},
    "boolean": "boolean",
    "bytea": "bytes",
    "text": "string",
    "varchar": "string",
    "char": "string",
    "uuid": {"type": "string", "logicalType": "uuid"},
    "date": {"type": "int", "logicalType": "date"},
    "timestamp": {"type": "long", "logicalType": "timestamp-millis"},
    "timestamptz": {"type": "long", "logicalType": "timestamp-millis"},
    "jsonb": "string",
    "json": "string",
}


def postgres_to_avro(pg_type: str, *, nullable: bool = False) -> Any:
    """Resolve a Postgres type name to an Avro type literal/union."""
    if not pg_type:
        raise ValueError("pg_type must be non-empty")
    key = pg_type.lower().strip()
    base: Any = _POSTGRES_TO_AVRO.get(key, {"type": "string", "logicalType": "unknown"})
    if nullable:
        return ["null", base]
    return base


def generate_avro_schema(
    *,
    namespace: str,
    name: str,
    columns: Sequence[tuple[str, str, bool]],
) -> dict[str, Any]:
    """Build an Avro record schema for a flattened row.

    ``columns`` is ``[(column_name, postgres_type, nullable), ...]``.
    """
    if not namespace:
        raise ValueError("namespace must be non-empty")
    if not name:
        raise ValueError("name must be non-empty")
    if not columns:
        raise ValueError("columns must be non-empty")
    seen: set[str] = set()
    fields: list[dict[str, Any]] = []
    for col, pg_type, nullable in columns:
        if not col:
            raise ValueError("column name must be non-empty")
        if col in seen:
            raise ValueError(f"duplicate column {col!r}")
        seen.add(col)
        field: dict[str, Any] = {"name": col, "type": postgres_to_avro(pg_type, nullable=nullable)}
        if nullable:
            field["default"] = None
        fields.append(field)
    return {
        "type": "record",
        "name": name,
        "namespace": namespace,
        "fields": fields,
    }


__all__ = ["generate_avro_schema", "postgres_to_avro"]
