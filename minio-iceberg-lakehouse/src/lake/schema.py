"""Schema with field IDs + evolution rules.

Iceberg's defining property is *schema evolution by field id, not by
position*. Each field has a stable numeric id assigned at first
appearance; renaming or reordering columns is a metadata-only change
because data files are keyed by id.

We support the safe subset of evolutions:

  * ``add_column`` — assigns a fresh id; the new column reads back as
    NULL on older snapshots.
  * ``drop_column`` — by id; reads continue to work on older data
    because the id is simply ignored.
  * ``rename_column`` — by id; pure metadata.

Disallowed:

  * Promoting types that lose precision (``double → int``).
  * Reassigning a field id to a different name+type pair.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class FieldType(str, Enum):
    """Minimal Iceberg-flavoured primitive types."""

    BOOLEAN = "boolean"
    INT = "int"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    DATE = "date"
    TIMESTAMP = "timestamp"


_PROMOTIONS: dict[FieldType, set[FieldType]] = {
    FieldType.INT: {FieldType.INT, FieldType.LONG},
    FieldType.LONG: {FieldType.LONG},
    FieldType.FLOAT: {FieldType.FLOAT, FieldType.DOUBLE},
    FieldType.DOUBLE: {FieldType.DOUBLE},
    FieldType.STRING: {FieldType.STRING},
    FieldType.BOOLEAN: {FieldType.BOOLEAN},
    FieldType.DATE: {FieldType.DATE},
    FieldType.TIMESTAMP: {FieldType.TIMESTAMP},
}


class SchemaEvolutionError(ValueError):
    """Raised when an evolution would silently corrupt data."""


@dataclass(frozen=True, slots=True)
class Field:
    """A schema column."""

    id: int
    name: str
    type: FieldType
    required: bool = False

    def __post_init__(self) -> None:
        if self.id < 1:
            raise ValueError("field id must be ≥ 1")
        if not self.name:
            raise ValueError("field name must be non-empty")


@dataclass(frozen=True, slots=True)
class Schema:
    """An ordered tuple of :class:`Field` plus a monotonic ``schema_id``."""

    schema_id: int
    fields: tuple[Field, ...]

    def __post_init__(self) -> None:
        if self.schema_id < 0:
            raise ValueError("schema_id must be ≥ 0")
        if not self.fields:
            raise ValueError("schema must have ≥ 1 field")
        if len({f.id for f in self.fields}) != len(self.fields):
            raise ValueError("duplicate field id in schema")
        if len({f.name for f in self.fields}) != len(self.fields):
            raise ValueError("duplicate field name in schema")

    def field_by_id(self, fid: int) -> Field:
        for f in self.fields:
            if f.id == fid:
                return f
        raise KeyError(f"field id {fid} not in schema")

    def field_by_name(self, name: str) -> Field:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(f"field {name!r} not in schema")

    # -------------------------------------------------------- evolution

    def add_column(self, name: str, ftype: FieldType, *, required: bool = False) -> Schema:
        if any(f.name == name for f in self.fields):
            raise SchemaEvolutionError(f"add_column: name {name!r} already present")
        new_id = max(f.id for f in self.fields) + 1
        return Schema(
            schema_id=self.schema_id + 1,
            fields=(*self.fields, Field(id=new_id, name=name, type=ftype, required=required)),
        )

    def drop_column(self, name: str) -> Schema:
        target = self.field_by_name(name)
        new_fields = tuple(f for f in self.fields if f.id != target.id)
        if not new_fields:
            raise SchemaEvolutionError("cannot drop the last column")
        return Schema(schema_id=self.schema_id + 1, fields=new_fields)

    def rename_column(self, name: str, new_name: str) -> Schema:
        if not new_name:
            raise SchemaEvolutionError("new_name must be non-empty")
        target = self.field_by_name(name)
        if any(f.name == new_name for f in self.fields):
            raise SchemaEvolutionError(f"rename: {new_name!r} already present")
        new_fields = tuple(
            Field(
                id=f.id,
                name=(new_name if f.id == target.id else f.name),
                type=f.type,
                required=f.required,
            )
            for f in self.fields
        )
        return Schema(schema_id=self.schema_id + 1, fields=new_fields)

    def promote_type(self, name: str, new_type: FieldType) -> Schema:
        target = self.field_by_name(name)
        if new_type not in _PROMOTIONS[target.type]:
            raise SchemaEvolutionError(
                f"cannot promote {target.type.value} → {new_type.value} (lossy)"
            )
        new_fields = tuple(
            Field(
                id=f.id,
                name=f.name,
                type=(new_type if f.id == target.id else f.type),
                required=f.required,
            )
            for f in self.fields
        )
        return Schema(schema_id=self.schema_id + 1, fields=new_fields)


__all__ = ["Field", "FieldType", "Schema", "SchemaEvolutionError"]
