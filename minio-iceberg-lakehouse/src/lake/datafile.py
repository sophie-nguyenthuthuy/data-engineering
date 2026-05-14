"""Data-file record (one row group / Parquet file in real Iceberg)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class DataFile:
    """Reference to a physical data file + per-column lightweight stats."""

    path: str
    record_count: int
    file_size_bytes: int
    partition: dict[str, Any] = field(default_factory=dict)
    column_min: dict[int, Any] = field(default_factory=dict)  # field-id → min
    column_max: dict[int, Any] = field(default_factory=dict)  # field-id → max
    null_counts: dict[int, int] = field(default_factory=dict)  # field-id → null count

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path must be non-empty")
        if self.record_count < 0:
            raise ValueError("record_count must be ≥ 0")
        if self.file_size_bytes < 0:
            raise ValueError("file_size_bytes must be ≥ 0")
        for fid, cnt in self.null_counts.items():
            if cnt < 0:
                raise ValueError(f"null_count for field {fid} must be ≥ 0")
            if cnt > self.record_count:
                raise ValueError(
                    f"null_count {cnt} for field {fid} exceeds record_count {self.record_count}"
                )


__all__ = ["DataFile"]
