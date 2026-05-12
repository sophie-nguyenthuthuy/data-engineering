"""Domain models for the data catalog metadata."""

import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


@dataclass
class Column:
    name: str
    data_type: str
    description: str = ""
    nullable: bool = True
    is_primary_key: bool = False
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Column":
        return cls(**d)


@dataclass
class Table:
    name: str
    dataset_name: str
    description: str = ""
    columns: List[Column] = field(default_factory=list)
    owner: str = ""
    tags: List[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None

    @property
    def full_name(self) -> str:
        return f"{self.dataset_name}.{self.name}"

    def to_dict(self) -> dict:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Table":
        cols = [Column.from_dict(c) for c in d.pop("columns", [])]
        obj = cls(**d)
        obj.columns = cols
        return obj


@dataclass
class Dataset:
    name: str
    description: str = ""
    location: str = ""   # e.g. s3://bucket/path, gs://...
    owner: str = ""
    tags: List[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Dataset":
        return cls(**d)


@dataclass
class DataLineage:
    """Directed edge: source_table → target_table, produced by a job."""
    source: str       # full table name
    target: str       # full table name
    job: str          # transform / pipeline name
    description: str = ""
    created_at: float = field(default_factory=time.time)

    @property
    def edge_id(self) -> str:
        return f"{self.source}→{self.target}@{self.job}"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "DataLineage":
        return cls(**d)
