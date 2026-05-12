"""Workload generator — produces and executes operation sequences against an engine."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field

import numpy as np
import pyarrow as pa

from cow_mor_bench.data.generator import generate_table, generate_update_batch, primary_key_for
from cow_mor_bench.engines.base import ReadResult, StorageEngine, WriteResult
from cow_mor_bench.workload.patterns import WorkloadProfile


@dataclass
class OperationRecord:
    op: str
    duration_s: float
    rows: int
    bytes_io: int
    files: int
    delta_files: int = 0


@dataclass
class WorkloadTrace:
    profile_name: str
    engine_strategy: str
    schema_name: str
    operations: list[OperationRecord] = field(default_factory=list)

    # Aggregates computed by summarise()
    total_write_s: float = 0.0
    total_read_s: float = 0.0
    total_compact_s: float = 0.0
    write_ops: int = 0
    read_ops: int = 0
    total_rows_written: int = 0
    total_rows_read: int = 0
    total_bytes_written: int = 0
    total_bytes_read: int = 0
    p50_write_ms: float = 0.0
    p95_write_ms: float = 0.0
    p50_read_ms: float = 0.0
    p95_read_ms: float = 0.0
    avg_delta_files_per_read: float = 0.0

    def summarise(self) -> None:
        write_ops = [o for o in self.operations if o.op in ("insert", "update", "delete", "create")]
        read_ops = [o for o in self.operations if o.op in ("full_scan", "point_lookup", "range_scan")]
        compact_ops = [o for o in self.operations if o.op in ("compact",)]

        self.total_write_s = sum(o.duration_s for o in write_ops)
        self.total_read_s = sum(o.duration_s for o in read_ops)
        self.total_compact_s = sum(o.duration_s for o in compact_ops)
        self.write_ops = len(write_ops)
        self.read_ops = len(read_ops)
        self.total_rows_written = sum(o.rows for o in write_ops)
        self.total_rows_read = sum(o.rows for o in read_ops)
        self.total_bytes_written = sum(o.bytes_io for o in write_ops)
        self.total_bytes_read = sum(o.bytes_io for o in read_ops)

        if write_ops:
            w_ms = np.array([o.duration_s * 1000 for o in write_ops])
            self.p50_write_ms = float(np.percentile(w_ms, 50))
            self.p95_write_ms = float(np.percentile(w_ms, 95))
        if read_ops:
            r_ms = np.array([o.duration_s * 1000 for o in read_ops])
            self.p50_read_ms = float(np.percentile(r_ms, 50))
            self.p95_read_ms = float(np.percentile(r_ms, 95))
            self.avg_delta_files_per_read = float(
                np.mean([o.delta_files for o in read_ops])
            )


class WorkloadGenerator:
    def __init__(
        self,
        engine: StorageEngine,
        profile: WorkloadProfile,
        schema_name: str,
        table_size: int = 50_000,
        n_ops: int = 100,
        compact_every: int | None = None,
        seed: int = 0,
    ):
        self._engine = engine
        self._profile = profile
        self._schema_name = schema_name
        self._table_size = table_size
        self._n_ops = n_ops
        self._compact_every = compact_every
        self._pk = primary_key_for(schema_name)
        self._rng = np.random.default_rng(seed)
        self._next_id = table_size + 1

    def _choose_op(self) -> str:
        p = self._profile
        weights = [
            (p.insert_weight, "insert"),
            (p.update_weight, "update"),
            (p.delete_weight, "delete"),
            (p.full_scan_weight, "full_scan"),
            (p.point_read_weight, "point_lookup"),
            (p.range_scan_weight, "range_scan"),
        ]
        ops, probs = zip(*[(o, w) for w, o in weights])
        probs_arr = np.array(probs, dtype=float)
        probs_arr /= probs_arr.sum()
        return str(self._rng.choice(ops, p=probs_arr))

    def run(self) -> WorkloadTrace:
        trace = WorkloadTrace(
            profile_name=self._profile.name,
            engine_strategy=self._engine.__class__.__name__,
            schema_name=self._schema_name,
        )

        # Seed the table
        initial_data = generate_table(
            self._schema_name, self._table_size, start_id=1,
            seed=int(self._rng.integers(0, 2**31)),
        )
        res = self._engine.create_table(initial_data)
        trace.operations.append(OperationRecord(
            op="create", duration_s=res.duration_s, rows=res.rows_written,
            bytes_io=res.bytes_written, files=res.files_written,
        ))

        current_table = initial_data
        compact_counter = 0

        for i in range(self._n_ops):
            op = self._choose_op()

            if op == "insert":
                n = max(1, int(self._rng.integers(
                    self._profile.rows_per_write // 2,
                    self._profile.rows_per_write * 2,
                )))
                rows = generate_table(
                    self._schema_name, n, start_id=self._next_id,
                    seed=int(self._rng.integers(0, 2**31)),
                )
                self._next_id += n
                r = self._engine.insert(rows)
                trace.operations.append(OperationRecord(
                    op="insert", duration_s=r.duration_s, rows=r.rows_written,
                    bytes_io=r.bytes_written, files=r.files_written,
                ))

            elif op == "update":
                updated = generate_update_batch(
                    current_table, self._profile.update_fraction,
                    self._schema_name, seed=int(self._rng.integers(0, 2**31)),
                )
                r = self._engine.update(updated)
                trace.operations.append(OperationRecord(
                    op="update", duration_s=r.duration_s, rows=r.rows_written,
                    bytes_io=r.bytes_written, files=r.files_written,
                ))

            elif op == "delete":
                n_del = max(1, int(self._table_size * 0.005))
                pk_vals = self._rng.integers(1, self._table_size, size=n_del).tolist()
                r = self._engine.delete([int(v) for v in pk_vals])
                trace.operations.append(OperationRecord(
                    op="delete", duration_s=r.duration_s, rows=0,
                    bytes_io=r.bytes_written, files=r.files_written,
                ))

            elif op == "full_scan":
                r = self._engine.full_scan()
                trace.operations.append(OperationRecord(
                    op="full_scan", duration_s=r.duration_s, rows=r.rows_returned,
                    bytes_io=r.bytes_scanned, files=r.files_scanned,
                    delta_files=r.delta_files_merged,
                ))

            elif op == "point_lookup":
                pk_val = int(self._rng.integers(1, self._table_size))
                r = self._engine.point_lookup(pk_val)
                trace.operations.append(OperationRecord(
                    op="point_lookup", duration_s=r.duration_s, rows=r.rows_returned,
                    bytes_io=r.bytes_scanned, files=r.files_scanned,
                    delta_files=r.delta_files_merged,
                ))

            elif op == "range_scan":
                lo = int(self._rng.integers(1, self._table_size - 1))
                hi = lo + int(self._rng.integers(100, self._table_size // 10))
                r = self._engine.range_scan(lo, hi)
                trace.operations.append(OperationRecord(
                    op="range_scan", duration_s=r.duration_s, rows=r.rows_returned,
                    bytes_io=r.bytes_scanned, files=r.files_scanned,
                    delta_files=r.delta_files_merged,
                ))

            compact_counter += 1
            if self._compact_every and compact_counter >= self._compact_every:
                cr = self._engine.compact()
                trace.operations.append(OperationRecord(
                    op="compact", duration_s=cr.duration_s, rows=cr.rows_written,
                    bytes_io=cr.bytes_written, files=cr.files_written,
                ))
                compact_counter = 0

        trace.summarise()
        return trace
