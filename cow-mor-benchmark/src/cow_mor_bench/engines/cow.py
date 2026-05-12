"""Copy-on-Write storage engine.

On every UPDATE or DELETE the engine reads all affected Parquet files,
applies the mutation in memory, writes new files, and removes the old ones.
INSERT simply appends a new file. Reads are a straight merge of the current
file set — no delta merging required.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from cow_mor_bench.data.generator import primary_key_for
from cow_mor_bench.data.schemas import (
    DataFile,
    Snapshot,
    TableMetadata,
    WriteStrategy,
)
from cow_mor_bench.engines.base import ReadResult, StorageEngine, TableStats, WriteResult

_TARGET_FILE_ROWS = 100_000  # split inserts above this threshold


class CopyOnWriteEngine(StorageEngine):
    def __init__(self, table_path: str, schema_name: str):
        super().__init__(table_path, schema_name)
        self._pk = primary_key_for(schema_name)
        self._data_dir = Path(table_path) / "data"
        self._data_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _file_id(self) -> str:
        return str(uuid.uuid4()).replace("-", "")[:16]

    def _write_parquet(self, table: pa.Table) -> DataFile:
        fid = self._file_id()
        path = str(self._data_dir / f"{fid}.parquet")
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

    def _load_metadata(self) -> TableMetadata:
        return TableMetadata.load(self.table_path)

    def _current_files(self) -> list[DataFile]:
        meta = self._load_metadata()
        snap = meta.current_snapshot()
        return snap.data_files if snap else []

    def _commit(self, data_files: list[DataFile], summary: dict) -> None:
        meta = self._load_metadata()
        meta.new_snapshot(data_files=data_files, delta_files=[], summary=summary)

    # ------------------------------------------------------------------
    # Engine interface
    # ------------------------------------------------------------------

    def create_table(self, initial_data: pa.Table) -> WriteResult:
        t0 = time.perf_counter()
        meta = TableMetadata(
            table_name=Path(self.table_path).name,
            base_path=self.table_path,
            strategy=WriteStrategy.COW,
            schema_name=self.schema_name,
        )
        meta.save()

        # Split large initial loads into multiple files
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
        existing = self._current_files()
        new_files: list[DataFile] = []
        total_bytes = 0

        for chunk_start in range(0, len(rows), _TARGET_FILE_ROWS):
            chunk = rows.slice(chunk_start, _TARGET_FILE_ROWS)
            df = self._write_parquet(chunk)
            new_files.append(df)
            total_bytes += df.size_bytes

        self._commit(
            existing + new_files,
            {"operation": "insert", "rows": len(rows)},
        )
        return WriteResult(
            rows_written=len(rows),
            files_written=len(new_files),
            files_rewritten=0,
            bytes_written=total_bytes,
            duration_s=time.perf_counter() - t0,
            operation="insert",
        )

    def update(self, updated_rows: pa.Table) -> WriteResult:
        t0 = time.perf_counter()
        pk_set = set(updated_rows.column(self._pk).to_pylist())
        existing = self._current_files()

        unchanged: list[DataFile] = []
        to_rewrite: list[DataFile] = []
        for df in existing:
            if df.max_pk >= min(pk_set) and df.min_pk <= max(pk_set):
                to_rewrite.append(df)
            else:
                unchanged.append(df)

        rewritten: list[DataFile] = []
        total_bytes = 0
        update_map: dict[int, dict] = {
            row[self._pk]: row
            for row in updated_rows.to_pylist()
        }

        for df in to_rewrite:
            base = pq.read_table(df.path)
            rows_list = base.to_pylist()
            for r in rows_list:
                if r[self._pk] in update_map:
                    r.update(update_map[r[self._pk]])
            new_table = pa.Table.from_pylist(rows_list, schema=base.schema)
            new_df = self._write_parquet(new_table)
            rewritten.append(new_df)
            total_bytes += new_df.size_bytes
            os.remove(df.path)

        self._commit(
            unchanged + rewritten,
            {"operation": "update", "rows": len(updated_rows)},
        )
        return WriteResult(
            rows_written=len(updated_rows),
            files_written=len(rewritten),
            files_rewritten=len(to_rewrite),
            bytes_written=total_bytes,
            duration_s=time.perf_counter() - t0,
            operation="update",
        )

    def delete(self, pk_values: list[int]) -> WriteResult:
        t0 = time.perf_counter()
        pk_set = set(pk_values)
        pk_min, pk_max = min(pk_set), max(pk_set)
        existing = self._current_files()

        unchanged: list[DataFile] = []
        to_rewrite: list[DataFile] = []
        for df in existing:
            if df.max_pk >= pk_min and df.min_pk <= pk_max:
                to_rewrite.append(df)
            else:
                unchanged.append(df)

        rewritten: list[DataFile] = []
        total_bytes = 0
        rows_deleted = 0

        for df in to_rewrite:
            base = pq.read_table(df.path)
            pk_col = base.column(self._pk)
            mask = pc.is_in(pk_col, value_set=pa.array(list(pk_set)))
            keep = pc.invert(mask)
            filtered = base.filter(keep)
            rows_deleted += len(base) - len(filtered)
            if len(filtered) > 0:
                new_df = self._write_parquet(filtered)
                rewritten.append(new_df)
                total_bytes += new_df.size_bytes
            os.remove(df.path)

        self._commit(
            unchanged + rewritten,
            {"operation": "delete", "rows": rows_deleted},
        )
        return WriteResult(
            rows_written=0,
            files_written=len(rewritten),
            files_rewritten=len(to_rewrite),
            bytes_written=total_bytes,
            duration_s=time.perf_counter() - t0,
            operation="delete",
        )

    def full_scan(self, filter_expr: str | None = None) -> ReadResult:
        t0 = time.perf_counter()
        files = self._current_files()
        if not files:
            return ReadResult(0, 0, 0, 0, time.perf_counter() - t0, "full_scan")

        paths = [f.path for f in files]
        bytes_scanned = sum(f.size_bytes for f in files)

        con = duckdb.connect()
        if filter_expr:
            sql = f"SELECT * FROM read_parquet({paths!r}) WHERE {filter_expr}"
        else:
            sql = f"SELECT * FROM read_parquet({paths!r})"
        result = con.execute(sql).fetchall()
        con.close()

        return ReadResult(
            rows_returned=len(result),
            files_scanned=len(files),
            bytes_scanned=bytes_scanned,
            delta_files_merged=0,
            duration_s=time.perf_counter() - t0,
            query=filter_expr or "full_scan",
        )

    def point_lookup(self, pk_value: int) -> ReadResult:
        t0 = time.perf_counter()
        files = self._current_files()
        candidates = [f for f in files if f.min_pk <= pk_value <= f.max_pk]
        if not candidates:
            return ReadResult(0, 0, 0, 0, time.perf_counter() - t0, f"pk={pk_value}")

        paths = [f.path for f in candidates]
        bytes_scanned = sum(f.size_bytes for f in candidates)
        con = duckdb.connect()
        sql = f"SELECT * FROM read_parquet({paths!r}) WHERE {self._pk} = {pk_value}"
        result = con.execute(sql).fetchall()
        con.close()

        return ReadResult(
            rows_returned=len(result),
            files_scanned=len(candidates),
            bytes_scanned=bytes_scanned,
            delta_files_merged=0,
            duration_s=time.perf_counter() - t0,
            query=f"{self._pk}={pk_value}",
        )

    def range_scan(self, pk_min: int, pk_max: int) -> ReadResult:
        t0 = time.perf_counter()
        files = self._current_files()
        candidates = [f for f in files if f.max_pk >= pk_min and f.min_pk <= pk_max]
        if not candidates:
            return ReadResult(0, 0, 0, 0, time.perf_counter() - t0, f"range {pk_min}-{pk_max}")

        paths = [f.path for f in candidates]
        bytes_scanned = sum(f.size_bytes for f in candidates)
        con = duckdb.connect()
        sql = (
            f"SELECT * FROM read_parquet({paths!r})"
            f" WHERE {self._pk} BETWEEN {pk_min} AND {pk_max}"
        )
        result = con.execute(sql).fetchall()
        con.close()

        return ReadResult(
            rows_returned=len(result),
            files_scanned=len(candidates),
            bytes_scanned=bytes_scanned,
            delta_files_merged=0,
            duration_s=time.perf_counter() - t0,
            query=f"{self._pk} BETWEEN {pk_min} AND {pk_max}",
        )

    def stats(self) -> TableStats:
        files = self._current_files()
        total_data = sum(f.size_bytes for f in files)
        total_rows = sum(f.row_count for f in files)
        return TableStats(
            data_file_count=len(files),
            delta_file_count=0,
            total_data_bytes=total_data,
            total_delta_bytes=0,
            total_rows=total_rows,
            amplification_ratio=1.0,
        )

    def compact(self) -> WriteResult:
        """CoW compaction = re-sort and merge small files into target-size files."""
        t0 = time.perf_counter()
        files = self._current_files()
        if len(files) <= 1:
            return WriteResult(0, 0, 0, 0, time.perf_counter() - t0, "compact_noop")

        parts = [pq.read_table(f.path) for f in files]
        merged = pa.concat_tables(parts)
        # Sort by pk for better min/max pruning
        merged = merged.sort_by(self._pk)

        new_files: list[DataFile] = []
        total_bytes = 0
        for old in files:
            os.remove(old.path)
        for chunk_start in range(0, len(merged), _TARGET_FILE_ROWS):
            chunk = merged.slice(chunk_start, _TARGET_FILE_ROWS)
            df = self._write_parquet(chunk)
            new_files.append(df)
            total_bytes += df.size_bytes

        self._commit(new_files, {"operation": "compact", "files_merged": len(files)})
        return WriteResult(
            rows_written=len(merged),
            files_written=len(new_files),
            files_rewritten=len(files),
            bytes_written=total_bytes,
            duration_s=time.perf_counter() - t0,
            operation="compact",
        )
