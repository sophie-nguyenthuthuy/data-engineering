"""Shared data schemas and table metadata structures."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import pyarrow as pa


class WriteStrategy(str, Enum):
    COW = "copy_on_write"
    MOR = "merge_on_read"


class OperationType(str, Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


# Standard benchmark table schema
ORDERS_SCHEMA = pa.schema([
    pa.field("order_id", pa.int64()),
    pa.field("customer_id", pa.int64()),
    pa.field("product_id", pa.int64()),
    pa.field("quantity", pa.int32()),
    pa.field("unit_price", pa.float64()),
    pa.field("total_amount", pa.float64()),
    pa.field("status", pa.string()),
    pa.field("region", pa.string()),
    pa.field("created_at", pa.timestamp("us")),
    pa.field("updated_at", pa.timestamp("us")),
])

EVENTS_SCHEMA = pa.schema([
    pa.field("event_id", pa.int64()),
    pa.field("user_id", pa.int64()),
    pa.field("session_id", pa.string()),
    pa.field("event_type", pa.string()),
    pa.field("page", pa.string()),
    pa.field("duration_ms", pa.int64()),
    pa.field("timestamp", pa.timestamp("us")),
])

INVENTORY_SCHEMA = pa.schema([
    pa.field("product_id", pa.int64()),
    pa.field("warehouse_id", pa.int32()),
    pa.field("stock_qty", pa.int64()),
    pa.field("reserved_qty", pa.int64()),
    pa.field("reorder_point", pa.int64()),
    pa.field("last_updated", pa.timestamp("us")),
])

DELTA_LOG_SCHEMA = pa.schema([
    pa.field("_op", pa.string()),          # insert / update / delete
    pa.field("_commit_ts", pa.int64()),    # epoch microseconds
    pa.field("_row_id", pa.int64()),       # target row primary key
    pa.field("_file_path", pa.string()),   # source data file (for deletes/updates)
    pa.field("_payload", pa.string()),     # JSON-encoded row data (for inserts/updates)
])


@dataclass
class DataFile:
    path: str
    row_count: int
    size_bytes: int
    min_pk: int
    max_pk: int
    snapshot_id: str


@dataclass
class DeltaFile:
    path: str
    row_count: int
    size_bytes: int
    commit_ts: int
    ops: dict[str, int] = field(default_factory=dict)  # {insert:n, update:n, delete:n}


@dataclass
class Snapshot:
    snapshot_id: str
    parent_id: str | None
    commit_ts: int
    data_files: list[DataFile] = field(default_factory=list)
    delta_files: list[DeltaFile] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "snapshot_id": self.snapshot_id,
            "parent_id": self.parent_id,
            "commit_ts": self.commit_ts,
            "data_files": [vars(f) for f in self.data_files],
            "delta_files": [vars(f) for f in self.delta_files],
            "summary": self.summary,
        }


@dataclass
class TableMetadata:
    table_name: str
    base_path: str
    strategy: WriteStrategy
    schema_name: str
    current_snapshot_id: str | None = None
    snapshots: list[Snapshot] = field(default_factory=list)

    @property
    def metadata_path(self) -> Path:
        return Path(self.base_path) / "metadata" / "table.json"

    def save(self) -> None:
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "table_name": self.table_name,
            "base_path": self.base_path,
            "strategy": self.strategy.value,
            "schema_name": self.schema_name,
            "current_snapshot_id": self.current_snapshot_id,
            "snapshots": [s.to_dict() for s in self.snapshots],
        }
        self.metadata_path.write_text(json.dumps(data, indent=2))

    @classmethod
    def load(cls, path: str) -> "TableMetadata":
        meta_path = Path(path) / "metadata" / "table.json"
        data = json.loads(meta_path.read_text())
        snapshots = []
        for s in data.get("snapshots", []):
            snap = Snapshot(
                snapshot_id=s["snapshot_id"],
                parent_id=s.get("parent_id"),
                commit_ts=s["commit_ts"],
                data_files=[DataFile(**f) for f in s.get("data_files", [])],
                delta_files=[DeltaFile(**f) for f in s.get("delta_files", [])],
                summary=s.get("summary", {}),
            )
            snapshots.append(snap)
        return cls(
            table_name=data["table_name"],
            base_path=data["base_path"],
            strategy=WriteStrategy(data["strategy"]),
            schema_name=data["schema_name"],
            current_snapshot_id=data.get("current_snapshot_id"),
            snapshots=snapshots,
        )

    def current_snapshot(self) -> Snapshot | None:
        if not self.current_snapshot_id:
            return None
        for snap in self.snapshots:
            if snap.snapshot_id == self.current_snapshot_id:
                return snap
        return None

    def new_snapshot(self, data_files: list[DataFile], delta_files: list[DeltaFile],
                     summary: dict) -> Snapshot:
        snap = Snapshot(
            snapshot_id=str(uuid.uuid4()),
            parent_id=self.current_snapshot_id,
            commit_ts=int(time.time() * 1_000_000),
            data_files=data_files,
            delta_files=delta_files,
            summary=summary,
        )
        self.snapshots.append(snap)
        self.current_snapshot_id = snap.snapshot_id
        self.save()
        return snap


def make_snapshot_id() -> str:
    return str(uuid.uuid4())


SCHEMA_REGISTRY: dict[str, pa.Schema] = {
    "orders": ORDERS_SCHEMA,
    "events": EVENTS_SCHEMA,
    "inventory": INVENTORY_SCHEMA,
}
