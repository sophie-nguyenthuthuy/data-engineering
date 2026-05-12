from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class FieldType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    BYTES = "bytes"
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"

    @classmethod
    def from_str(cls, value: str) -> "FieldType":
        try:
            return cls(value.lower())
        except ValueError:
            raise ValueError(f"Unknown field type: {value!r}. Valid types: {[t.value for t in cls]}")

    def is_numeric(self) -> bool:
        return self in (self.INTEGER, self.LONG, self.FLOAT, self.DOUBLE)

    def is_temporal(self) -> bool:
        return self in (self.TIMESTAMP, self.DATE)


@dataclass
class FieldSchema:
    name: str
    dtype: FieldType
    nullable: bool = True
    metadata: dict = field(default_factory=dict)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FieldSchema):
            return NotImplemented
        return self.name == other.name and self.dtype == other.dtype and self.nullable == other.nullable

    def __repr__(self) -> str:
        null = "?" if self.nullable else "!"
        return f"{self.name}: {self.dtype.value}{null}"


@dataclass
class Schema:
    fields: list[FieldSchema]

    def __post_init__(self) -> None:
        names = [f.name for f in self.fields]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"Duplicate field names in schema: {dupes}")

    def field_names(self) -> list[str]:
        return [f.name for f in self.fields]

    def get_field(self, name: str) -> Optional[FieldSchema]:
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def project(self, names: list[str]) -> "Schema":
        missing = set(names) - set(self.field_names())
        if missing:
            raise ValueError(f"Fields not in schema: {missing}")
        return Schema([f for f in self.fields if f.name in names])

    def merge(self, other: "Schema", prefix: Optional[str] = None) -> "Schema":
        new_fields = list(self.fields)
        for f in other.fields:
            name = f"{prefix}.{f.name}" if prefix else f.name
            if name not in self.field_names():
                new_fields.append(FieldSchema(name, f.dtype, f.nullable))
        return Schema(new_fields)

    def is_compatible_with(self, other: "Schema") -> bool:
        self_map = {f.name: f for f in self.fields}
        other_map = {f.name: f for f in other.fields}
        for name, f in self_map.items():
            if name in other_map and other_map[name].dtype != f.dtype:
                return False
        return True

    def __repr__(self) -> str:
        return f"Schema({', '.join(repr(f) for f in self.fields)})"


_LATENCY_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_SIZE_UNITS = {"b": 1, "kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12}


def _parse_duration(s: str) -> int:
    """Return seconds. Accepts '30s', '5m', '2h', '1d', or raw int (seconds)."""
    s = s.strip().lower()
    for suffix, mult in sorted(_LATENCY_UNITS.items(), key=lambda x: -len(x[0])):
        if s.endswith(suffix):
            return int(float(s[: -len(suffix)]) * mult)
    return int(s)


def _parse_size(s: str) -> float:
    """Return GB. Accepts '500gb', '1.2tb', '200mb', or raw float (GB)."""
    s = s.strip().lower()
    for suffix, mult in sorted(_SIZE_UNITS.items(), key=lambda x: -len(x[0])):
        if s.endswith(suffix):
            return float(s[: -len(suffix)]) * mult / 1e9

    return float(s)


@dataclass
class SLA:
    max_latency: str = "24h"
    dataset_size: str = "10gb"

    def latency_seconds(self) -> int:
        return _parse_duration(self.max_latency)

    def dataset_size_gb(self) -> float:
        return _parse_size(self.dataset_size)

    def __repr__(self) -> str:
        return f"SLA(latency={self.max_latency}, size={self.dataset_size})"


class TransformType(str, Enum):
    SOURCE = "source"
    FILTER = "filter"
    MAP = "map"
    SELECT = "select"
    JOIN = "join"
    AGGREGATE = "aggregate"
    UNION = "union"
    WINDOW = "window"
    SINK = "sink"


class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


class AggFunction(str, Enum):
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    COLLECT_LIST = "collect_list"
    COUNT_DISTINCT = "count_distinct"


@dataclass
class Aggregation:
    output_name: str
    function: AggFunction
    column: Optional[str] = None

    def __repr__(self) -> str:
        col = f"({self.column})" if self.column else "()"
        return f"{self.output_name} = {self.function.value}{col}"
