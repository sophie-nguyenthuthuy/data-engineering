"""Merge-on-Read storage engine.

Mutations (INSERT/UPDATE/DELETE) are recorded as lightweight delta log files.
Reads merge base Parquet files with all outstanding delta entries on the fly.
COMPACT absorbs deltas back into the base layer, resetting the delta log.
"""

from __future__ import annotations

import datetime
import json
import os
import time
import uuid
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from cow_mor_bench.data.generator import primary_key_for
from cow_mor_bench.data.schemas import (
    DELTA_LOG_SCHEMA,
    DataFile,
    DeltaFile,
    TableMetadata,
    WriteStrategy,
)
from cow_mor_bench.engines.base import ReadResult, StorageEngine, TableStats, WriteResult

_TARGET_FILE_ROWS = 100_000


def _json_default(obj):
    if isinstance(obj, datetime.datetime):
        # Store as epoch microseconds so PyArrow can reconstruct timestamp("us")
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        ts = obj if obj.tzinfo else obj.replace(tzinfo=datetime.timezone.utc)
        return int((ts - epoch).total_seconds() * 1_000_000)
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
_DELTA_FLUSH_ROWS = 5_000     # max rows per delta file before a new one is started


class MergeOnReadEngine(StorageEngine):
    def __init__(self, table_path: str, schema_name: str):
        super().__init__(table_path, schema_name)
        self._pk = primary_key_for(schema_name)
        self._data_dir = Path(table_path) / "data"
        self._delta_dir = Path(table_path) / "delta"
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._delta_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _file_id(self) -> str:
        return str(uuid.uuid4()).replace("-", "")[:16]

    def _write_parquet(self, table: pa.Table, subdir: Path | None = None) -> DataFile:
        d = subdir or self._data_dir
        path = str(d / f"{self._file_id()}.parquet")
        pq.write_table(table, path, compression="snappy", row_group_size=50_000)
        size = os.path.getsize(path)
        pk_col = table.column(self._pk)
        return DataFile(
            path=path,
            row_count=len(table),
            size_bytes=size,
            min_pk=int(pc.min(pk_col).as_py()),
            max_pk=int(pc.max(pk_col).as_py()),
            snapshot_id="",
        )

    def _write_delta(self, op: str, rows: pa.Table | None, pk_values: list[int] | None) -> DeltaFile:
        now_us = int(time.time() * 1_000_000)
        path = str(self._delta_dir / f"delta_{now_us}_{self._file_id()}.parquet")

        if op == "delete" and pk_values:
            entries = pa.table({
                "_op": pa.array(["delete"] * len(pk_values), type=pa.string()),
                "_commit_ts": pa.array([now_us] * len(pk_values), type=pa.int64()),
                "_row_id": pa.array(pk_values, type=pa.int64()),
                "_file_path": pa.array([""] * len(pk_values), type=pa.string()),
                "_payload": pa.array([""] * len(pk_values), type=pa.string()),
            })
        else:
            assert rows is not None
            payloads = [json.dumps(r, default=_json_default) for r in rows.to_pylist()]
            pk_list = rows.column(self._pk).to_pylist()
            entries = pa.table({
                "_op": pa.array([op] * len(rows), type=pa.string()),
                "_commit_ts": pa.array([now_us] * len(rows), type=pa.int64()),
                "_row_id": pa.array(pk_list, type=pa.int64()),
                "_file_path": pa.array([""] * len(rows), type=pa.string()),
                "_payload": pa.array(payloads, type=pa.string()),
            })

        pq.write_table(entries, path, compression="snappy")
        size = os.path.getsize(path)
        op_counts = {op: len(pk_values) if op == "delete" else len(rows)}
        return DeltaFile(
            path=path,
            row_count=len(entries),
            size_bytes=size,
            commit_ts=now_us,
            ops=op_counts,
        )

    def _load_metadata(self) -> TableMetadata:
        return TableMetadata.load(self.table_path)

    def _current_state(self) -> tuple[list[DataFile], list[DeltaFile]]:
        meta = self._load_metadata()
        snap = meta.current_snapshot()
        if snap is None:
            return [], []
        return snap.data_files, snap.delta_files

    def _commit(self, data_files: list[DataFile], delta_files: list[DeltaFile], summary: dict) -> None:
        meta = self._load_metadata()
        meta.new_snapshot(data_files=data_files, delta_files=delta_files, summary=summary)

    def _merge_read(self, base_tables: list[pa.Table], delta_files: list[DeltaFile]) -> pa.Table:
        """Apply delta log entries to base tables and return merged result."""
        if not base_tables:
            return pa.table({})

        merged = pa.concat_tables(base_tables)

        if not delta_files:
            return merged

        # Read all delta entries sorted by commit time
        delta_parts = [pq.read_table(df.path) for df in sorted(delta_files, key=lambda d: d.commit_ts)]
        delta_all = pa.concat_tables(delta_parts)

        rows = {r[self._pk]: r for r in merged.to_pylist()}
        schema = merged.schema

        for entry in sorted(delta_all.to_pylist(), key=lambda e: e["_commit_ts"]):
            op = entry["_op"]
            row_id = entry["_row_id"]
            if op == "delete":
                rows.pop(row_id, None)
            elif op in ("insert", "update"):
                payload = json.loads(entry["_payload"])
                rows[row_id] = payload

        if not rows:
            return pa.table({col: pa.array([], type=schema.field(col).type) for col in schema.names})

        return pa.Table.from_pylist(list(rows.values()), schema=schema)

    # ------------------------------------------------------------------
    # Engine interface
    # ------------------------------------------------------------------

    def create_table(self, initial_data: pa.Table) -> WriteResult:
        t0 = time.perf_counter()
        meta = TableMetadata(
            table_name=Path(self.table_path).name,
            base_path=self.table_path,
            strategy=WriteStrategy.MOR,
            schema_name=self.schema_name,
        )
        meta.save()

        data_files: list[DataFile] = []
        total_bytes = 0
        for chunk_start in range(0, len(initial_data), _TARGET_FILE_ROWS):
            chunk = initial_data.slice(chunk_start, _TARGET_FILE_ROWS)
            df = self._write_parquet(chunk)
            data_files.append(df)
            total_bytes += df.size_bytes

        meta.new_snapshot(
            data_files=data_files,
            delta_files=[],
            summary={"operation": "create", "rows": len(initial_data)},
        )
        return WriteResult(
            rows_written=len(initial_data),
            files_written=len(data_files),
            files_rewritten=0,
            bytes_written=total_bytes,
            duration_s=time.perf_counter() - t0,
            operation="create",
        )

    def insert(self, rows: pa.Table) -> WriteResult:
        t0 = time.perf_counter()
        data_files, delta_files = self._current_state()

        # Write to delta log (append-only, fast)
        delta_f = self._write_delta("insert", rows, None)
        self._commit(
            data_files,
            delta_files + [delta_f],
            {"operation": "insert", "rows": len(rows)},
        )
        return WriteResult(
            rows_written=len(rows),
            files_written=1,
            files_rewritten=0,
            bytes_written=delta_f.size_bytes,
            duration_s=time.perf_counter() - t0,
            operation="insert",
        )

    def update(self, updated_rows: pa.Table) -> WriteResult:
        t0 = time.perf_counter()
        data_files, delta_files = self._current_state()

        delta_f = self._write_delta("update", updated_rows, None)
        self._commit(
            data_files,
            delta_files + [delta_f],
            {"operation": "update", "rows": len(updated_rows)},
        )
        return WriteResult(
            rows_written=len(updated_rows),
            files_written=1,
            files_rewritten=0,
            bytes_written=delta_f.size_bytes,
            duration_s=time.perf_counter() - t0,
            operation="update",
        )

    def delete(self, pk_values: list[int]) -> WriteResult:
        t0 = time.perf_counter()
        data_files, delta_files = self._current_state()

        delta_f = self._write_delta("delete", None, pk_values)
        self._commit(
            data_files,
            delta_files + [delta_f],
            {"operation": "delete", "rows": len(pk_values)},
        )
        return WriteResult(
            rows_written=0,
            files_written=1,
            files_rewritten=0,
            bytes_written=delta_f.size_bytes,
            duration_s=time.perf_counter() - t0,
            operation="delete",
        )

    def full_scan(self, filter_expr: str | None = None) -> ReadResult:
        t0 = time.perf_counter()
        data_files, delta_files = self._current_state()
        if not data_files and not delta_files:
            return ReadResult(0, 0, 0, 0, time.perf_counter() - t0, "full_scan")

        bytes_scanned = sum(f.size_bytes for f in data_files) + sum(d.size_bytes for d in delta_files)
        base_tables = [pq.read_table(f.path) for f in data_files]
        merged = self._merge_read(base_tables, delta_files)

        if filter_expr and len(merged) > 0:
            con = duckdb.connect()
            result_table = con.execute(f"SELECT * FROM merged WHERE {filter_expr}").arrow()
            con.close()
            rows_returned = len(result_table)
        else:
            rows_returned = len(merged)

        return ReadResult(
            rows_returned=rows_returned,
            files_scanned=len(data_files),
            bytes_scanned=bytes_scanned,
            delta_files_merged=len(delta_files),
            duration_s=time.perf_counter() - t0,
            query=filter_expr or "full_scan",
        )

    def point_lookup(self, pk_value: int) -> ReadResult:
        t0 = time.perf_counter()
        data_files, delta_files = self._current_state()

        candidates = [f for f in data_files if f.min_pk <= pk_value <= f.max_pk]
        bytes_scanned = sum(f.size_bytes for f in candidates) + sum(d.size_bytes for d in delta_files)
        base_tables = [pq.read_table(f.path) for f in candidates]
        merged = self._merge_read(base_tables, delta_files)

        if len(merged) > 0:
            con = duckdb.connect()
            result = con.execute(f"SELECT * FROM merged WHERE {self._pk} = {pk_value}").fetchall()
            con.close()
            rows_returned = len(result)
        else:
            rows_returned = 0

        return ReadResult(
            rows_returned=rows_returned,
            files_scanned=len(candidates),
            bytes_scanned=bytes_scanned,
            delta_files_merged=len(delta_files),
            duration_s=time.perf_counter() - t0,
            query=f"{self._pk}={pk_value}",
        )

    def range_scan(self, pk_min: int, pk_max: int) -> ReadResult:
        t0 = time.perf_counter()
        data_files, delta_files = self._current_state()

        candidates = [f for f in data_files if f.max_pk >= pk_min and f.min_pk <= pk_max]
        bytes_scanned = sum(f.size_bytes for f in candidates) + sum(d.size_bytes for d in delta_files)
        base_tables = [pq.read_table(f.path) for f in candidates]
        merged = self._merge_read(base_tables, delta_files)

        if len(merged) > 0:
            con = duckdb.connect()
            result = con.execute(
                f"SELECT * FROM merged WHERE {self._pk} BETWEEN {pk_min} AND {pk_max}"
            ).fetchall()
            con.close()
            rows_returned = len(result)
        else:
            rows_returned = 0

        return ReadResult(
            rows_returned=rows_returned,
            files_scanned=len(candidates),
            bytes_scanned=bytes_scanned,
            delta_files_merged=len(delta_files),
            duration_s=time.perf_counter() - t0,
            query=f"{self._pk} BETWEEN {pk_min} AND {pk_max}",
        )

    def stats(self) -> TableStats:
        data_files, delta_files = self._current_state()
        total_data = sum(f.size_bytes for f in data_files)
        total_delta = sum(d.size_bytes for d in delta_files)
        total_rows = sum(f.row_count for f in data_files)
        ratio = (total_data + total_delta) / max(total_data, 1)
        return TableStats(
            data_file_count=len(data_files),
            delta_file_count=len(delta_files),
            total_data_bytes=total_data,
            total_delta_bytes=total_delta,
            total_rows=total_rows,
            amplification_ratio=ratio,
        )

    def compact(self) -> WriteResult:
        """Merge delta log back into base files, resetting the delta layer."""
        t0 = time.perf_counter()
        data_files, delta_files = self._current_state()

        if not delta_files:
            return WriteResult(0, 0, 0, 0, time.perf_counter() - t0, "compact_noop")

        base_tables = [pq.read_table(f.path) for f in data_files]
        merged = self._merge_read(base_tables, delta_files)
        merged = merged.sort_by(self._pk)

        # Remove old files
        for f in data_files:
            os.remove(f.path)
        for d in delta_files:
            os.remove(d.path)

        new_data_files: list[DataFile] = []
        total_bytes = 0
        for chunk_start in range(0, len(merged), _TARGET_FILE_ROWS):
            chunk = merged.slice(chunk_start, _TARGET_FILE_ROWS)
            df = self._write_parquet(chunk)
            new_data_files.append(df)
            total_bytes += df.size_bytes

        self._commit(
            new_data_files,
            [],
            {
                "operation": "compact",
                "delta_files_absorbed": len(delta_files),
                "data_files_merged": len(data_files),
            },
        )
        return WriteResult(
            rows_written=len(merged),
            files_written=len(new_data_files),
            files_rewritten=len(data_files) + len(delta_files),
            bytes_written=total_bytes,
            duration_s=time.perf_counter() - t0,
            operation="compact",
        )
