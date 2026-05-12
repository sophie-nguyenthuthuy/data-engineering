"""SQL-ish type system.

Minimal but rigorous: each type carries (name, byte_width, nullable, is_numeric).
Used by the cost model to estimate row width and predicate evaluation cost,
and by the codegen to pick correct casts per engine dialect.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DataType:
    name: str
    byte_width: int
    is_numeric: bool = False
    is_string: bool = False
    is_temporal: bool = False
    nullable: bool = True

    def with_nullable(self, nullable: bool) -> DataType:
        if self.nullable == nullable:
            return self
        return DataType(
            name=self.name,
            byte_width=self.byte_width,
            is_numeric=self.is_numeric,
            is_string=self.is_string,
            is_temporal=self.is_temporal,
            nullable=nullable,
        )

    def __repr__(self) -> str:
        suffix = "" if self.nullable else " NOT NULL"
        return f"{self.name}{suffix}"


# Canonical type singletons. Use these everywhere — equality is by-name +
# nullability, so two `INT32`s are equal even when produced independently.
INT32 = DataType(name="INT32", byte_width=4, is_numeric=True)
INT64 = DataType(name="INT64", byte_width=8, is_numeric=True)
DOUBLE = DataType(name="DOUBLE", byte_width=8, is_numeric=True)
BOOLEAN = DataType(name="BOOLEAN", byte_width=1)
STRING = DataType(name="STRING", byte_width=24, is_string=True)  # 24-byte sso heuristic
TIMESTAMP = DataType(name="TIMESTAMP", byte_width=8, is_temporal=True)


_PROMOTION: dict[tuple[str, str], DataType] = {
    ("INT32", "INT32"): INT32,
    ("INT32", "INT64"): INT64,
    ("INT64", "INT32"): INT64,
    ("INT64", "INT64"): INT64,
    ("INT32", "DOUBLE"): DOUBLE,
    ("DOUBLE", "INT32"): DOUBLE,
    ("INT64", "DOUBLE"): DOUBLE,
    ("DOUBLE", "INT64"): DOUBLE,
    ("DOUBLE", "DOUBLE"): DOUBLE,
    ("STRING", "STRING"): STRING,
    ("BOOLEAN", "BOOLEAN"): BOOLEAN,
    ("TIMESTAMP", "TIMESTAMP"): TIMESTAMP,
}


def promote(a: DataType, b: DataType) -> DataType:
    """Return the common type for a binary arithmetic / comparison op.

    Raises TypeError if no compatible promotion exists.
    """
    key = (a.name, b.name)
    if key in _PROMOTION:
        return _PROMOTION[key].with_nullable(a.nullable or b.nullable)
    raise TypeError(f"cannot promote {a} with {b}")
