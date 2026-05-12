"""
Physical operators for vectorized columnar execution.

All operators work on pyarrow RecordBatches of BATCH_SIZE rows.
Vectorized predicates and expressions use pyarrow.compute (SIMD-backed).

Late materialization is implemented in SequentialScan:
  - evaluate pushed predicates on only the predicate columns
  - build a boolean selection mask
  - then fetch the remaining output columns and apply the mask
  This avoids reading wide columns for rows that will be filtered out.
"""
from __future__ import annotations

import collections
from abc import ABC, abstractmethod
from typing import Dict, Generator, Iterable, Iterator, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .catalog import Catalog, Table
from .expressions import AggExpr, BinaryExpr, ColumnRef, Expr, conjuncts_to_expr


BATCH_SIZE = 8192   # Tuned for L1/L2 cache; SIMD-friendly power of two


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class PhysicalOp(ABC):
    """Iterator / volcano-model interface."""

    @abstractmethod
    def open(self, catalog: Catalog) -> None: ...

    @abstractmethod
    def next(self) -> Optional[pa.RecordBatch]: ...

    @abstractmethod
    def close(self) -> None: ...

    # Convenience: collect all output into a single Table
    def collect(self, catalog: Catalog) -> pa.Table:
        self.open(catalog)
        batches = []
        while True:
            b = self.next()
            if b is None:
                break
            batches.append(b)
        self.close()
        if not batches:
            return pa.table({})
        return pa.Table.from_batches(batches)


# ---------------------------------------------------------------------------
# SequentialScan  (with late materialization + predicate pushdown)
# ---------------------------------------------------------------------------

class SequentialScan(PhysicalOp):
    """
    Reads a table in BATCH_SIZE chunks.

    Late materialization strategy:
      1. Read only the columns referenced by pushed predicates (predicate cols).
      2. Evaluate all predicates → boolean mask.
      3. Read the remaining output columns (late cols) and apply the mask.
      4. Yield the combined batch of only the rows that passed the filter.

    When there are no predicates, all columns are read in one pass.
    """

    def __init__(
        self,
        table_name: str,
        output_cols: Optional[List[str]],
        predicates: List[Expr],
    ) -> None:
        self.table_name = table_name
        self.output_cols = output_cols    # None = all columns
        self.predicates = predicates
        self._table: Optional[pa.Table] = None
        self._offset = 0

    def open(self, catalog: Catalog) -> None:
        self._table = catalog.get(self.table_name).data
        # Resolve output columns
        schema_names = self._table.schema.names
        if self.output_cols:
            # Keep only cols that actually exist in the table
            self._out_cols = [c for c in self.output_cols if c in schema_names]
        else:
            self._out_cols = schema_names
        # Columns used only for predicate evaluation (not in output)
        if self.predicates:
            pred_cols: set[str] = set()
            for p in self.predicates:
                pred_cols |= p.columns_used()
            self._pred_cols = [c for c in pred_cols if c in schema_names]
            self._late_cols = [c for c in self._out_cols if c not in pred_cols]
        else:
            self._pred_cols = []
            self._late_cols = []
        self._offset = 0

    def next(self) -> Optional[pa.RecordBatch]:
        if self._table is None or self._offset >= len(self._table):
            return None

        start = self._offset
        end = min(start + BATCH_SIZE, len(self._table))
        length = end - start
        self._offset = end

        if not self.predicates:
            # Fast path: no filtering needed
            chunk = self._table.slice(start, length)
            if self._out_cols:
                chunk = chunk.select(self._out_cols)
            return chunk.to_batches()[0]

        # --- Late materialization ---
        # Step 1: Read only predicate columns and evaluate the filter mask.
        #         This avoids fetching wide columns for rows that will be discarded.
        early_cols = self._pred_cols if self._pred_cols else self._out_cols[:1]
        pred_chunk = self._table.select(early_cols).slice(start, length)
        pred_batch = pred_chunk.to_batches()[0]

        # Step 2: Build combined boolean mask
        mask = None
        for p in self.predicates:
            try:
                m = p.eval(pred_batch)
                mask = m if mask is None else pc.and_(mask, m)
            except Exception:
                pass  # predicate references column not yet loaded; skip here

        if mask is None:
            # No predicate could be evaluated; return all rows
            chunk = self._table.slice(start, length)
            if self._out_cols:
                chunk = chunk.select(self._out_cols)
            return chunk.to_batches()[0]

        # Fast skip: entire batch filtered out
        passing = pc.sum(pc.cast(mask, pa.int32())).as_py()
        if passing == 0:
            return self.next()

        # Step 3: Apply mask to predicate columns, then fetch + apply mask to late columns.
        arrays: Dict[str, pa.Array] = {}
        for name in self._pred_cols:
            if name in self._out_cols:
                arrays[name] = pred_batch.column(name).filter(mask)

        if self._late_cols:
            late_slice = self._table.select(self._late_cols).slice(start, length)
            late_batch = late_slice.to_batches()[0]
            for name in self._late_cols:
                arrays[name] = late_batch.column(name).filter(mask)

        # Return columns in the requested output order
        final_names = [c for c in self._out_cols if c in arrays]
        if not final_names:
            # Nothing matched output columns (e.g. all cols were late and empty)
            return self.next()

        return pa.RecordBatch.from_arrays(
            [arrays[n] for n in final_names],
            names=final_names,
        )

    def close(self) -> None:
        self._table = None
        self._offset = 0


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------

class FilterOp(PhysicalOp):
    def __init__(self, child: PhysicalOp, predicate: Expr) -> None:
        self.child = child
        self.predicate = predicate

    def open(self, catalog: Catalog) -> None:
        self.child.open(catalog)

    def next(self) -> Optional[pa.RecordBatch]:
        while True:
            batch = self.child.next()
            if batch is None:
                return None
            mask = self.predicate.eval(batch)
            filtered = batch.filter(mask)
            if filtered.num_rows > 0:
                return filtered

    def close(self) -> None:
        self.child.close()


# ---------------------------------------------------------------------------
# Project
# ---------------------------------------------------------------------------

class ProjectOp(PhysicalOp):
    def __init__(
        self,
        child: PhysicalOp,
        exprs: List[Expr],
        aliases: List[Optional[str]],
    ) -> None:
        self.child = child
        self.exprs = exprs
        self.aliases = aliases

    def _name(self, i: int) -> str:
        alias = self.aliases[i] if i < len(self.aliases) else None
        if alias:
            return alias
        e = self.exprs[i]
        if isinstance(e, ColumnRef):
            return e.name
        return repr(e)

    def open(self, catalog: Catalog) -> None:
        self.child.open(catalog)

    def next(self) -> Optional[pa.RecordBatch]:
        batch = self.child.next()
        if batch is None:
            return None
        arrays = [e.eval(batch) for e in self.exprs]
        names = [self._name(i) for i in range(len(self.exprs))]
        return pa.RecordBatch.from_arrays(arrays, names=names)

    def close(self) -> None:
        self.child.close()


# ---------------------------------------------------------------------------
# Hash Aggregate  (pipeline breaker — accumulates all input)
# ---------------------------------------------------------------------------

class HashAggOp(PhysicalOp):
    """
    Two-phase hash aggregation:
      Phase 1 (build): consume all child batches, maintain per-group partial states.
      Phase 2 (probe): yield result batches.

    For global aggregates (no GROUP BY), a single virtual group '' is used.
    """

    def __init__(
        self,
        child: PhysicalOp,
        group_by: List[Expr],
        aggregates: List[AggExpr],
    ) -> None:
        self.child = child
        self.group_by = group_by
        self.aggregates = aggregates
        self._result: Optional[pa.RecordBatch] = None
        self._done = False

    def open(self, catalog: Catalog) -> None:
        self.child.open(catalog)
        self._result = None
        self._done = False

    def _build(self, catalog: Catalog) -> None:
        # state: group_key_tuple → [partial_state_per_agg]
        states: Dict[Tuple, List] = collections.defaultdict(
            lambda: [None] * len(self.aggregates)
        )

        batch = self.child.next()
        while batch is not None:
            n = batch.num_rows
            if n == 0:
                batch = self.child.next()
                continue

            if self.group_by:
                # Vectorized group key extraction
                key_cols = [e.eval(batch) for e in self.group_by]
                key_np = [col.to_pylist() for col in key_cols]

                # Per-row aggregation
                # For efficiency: group rows into buckets in Python
                # (a production engine would use a vectorized hash table)
                row_keys: List[Tuple] = list(zip(*key_np)) if key_np else [() for _ in range(n)]

                # Build partial per-batch aggregates per unique group
                from itertools import groupby as _groupby
                import numpy as _np

                # Build group → row indices mapping
                group_indices: Dict[Tuple, List[int]] = collections.defaultdict(list)
                for i, k in enumerate(row_keys):
                    group_indices[k].append(i)

                for key, indices in group_indices.items():
                    idx_array = pa.array(indices)
                    sub_batch = batch.take(idx_array)
                    for j, agg in enumerate(self.aggregates):
                        partial = agg.partial(sub_batch)
                        states[key][j] = agg.merge(states[key][j], partial)
            else:
                # Global aggregate — single virtual group
                key = ()
                for j, agg in enumerate(self.aggregates):
                    partial = agg.partial(batch)
                    states[key][j] = agg.merge(states[key][j], partial)

            batch = self.child.next()

        # Materialize result
        key_arrays: List[pa.Array] = [[] for _ in self.group_by]
        agg_arrays: List[List] = [[] for _ in self.aggregates]

        for key, agg_states in states.items():
            for i, v in enumerate(key):
                key_arrays[i].append(v)
            for j, agg in enumerate(self.aggregates):
                agg_arrays[j].append(agg.finalize(agg_states[j]))

        names: List[str] = []
        arrays: List[pa.Array] = []

        for i, e in enumerate(self.group_by):
            name = e.name if isinstance(e, ColumnRef) else repr(e)
            names.append(name)
            arrays.append(pa.array(key_arrays[i]))

        for j, agg in enumerate(self.aggregates):
            names.append(agg.output_name)
            arrays.append(pa.array(agg_arrays[j]))

        if not names:
            self._result = None
        else:
            self._result = pa.RecordBatch.from_arrays(arrays, names=names)

    def next(self) -> Optional[pa.RecordBatch]:
        if not self._done:
            self._build(None)   # catalog not needed here
            self._done = True
        if self._result is not None:
            r = self._result
            self._result = None
            return r
        return None

    def open(self, catalog: Catalog) -> None:
        self.child.open(catalog)
        self._done = False
        self._result = None

    def close(self) -> None:
        self.child.close()


# ---------------------------------------------------------------------------
# Sort  (pipeline breaker)
# ---------------------------------------------------------------------------

class SortOp(PhysicalOp):
    def __init__(
        self,
        child: PhysicalOp,
        keys: List[Expr],
        ascending: List[bool],
    ) -> None:
        self.child = child
        self.keys = keys
        self.ascending = ascending
        self._batches: List[pa.RecordBatch] = []
        self._result: Optional[pa.RecordBatch] = None
        self._done = False

    def open(self, catalog: Catalog) -> None:
        self.child.open(catalog)
        self._batches = []
        self._result = None
        self._done = False

    def _build(self) -> None:
        batches = []
        b = self.child.next()
        while b is not None:
            if b.num_rows > 0:
                batches.append(b)
            b = self.child.next()

        if not batches:
            return

        tbl = pa.Table.from_batches(batches)

        sort_keys = []
        for k, asc in zip(self.keys, self.ascending):
            name = k.name if isinstance(k, ColumnRef) else repr(k)
            if name in tbl.schema.names:
                sort_keys.append((name, "ascending" if asc else "descending"))

        if sort_keys:
            indices = pc.sort_indices(tbl, sort_keys=sort_keys)
            tbl = tbl.take(indices)

        batches_out = tbl.to_batches(max_chunksize=BATCH_SIZE)
        self._batches = batches_out

    def next(self) -> Optional[pa.RecordBatch]:
        if not self._done:
            self._build()
            self._done = True
        if not self._batches:
            return None
        return self._batches.pop(0)

    def close(self) -> None:
        self.child.close()


# ---------------------------------------------------------------------------
# Limit
# ---------------------------------------------------------------------------

class LimitOp(PhysicalOp):
    def __init__(self, child: PhysicalOp, n: int, offset: int = 0) -> None:
        self.child = child
        self.n = n
        self.offset = offset
        self._skipped = 0
        self._emitted = 0

    def open(self, catalog: Catalog) -> None:
        self.child.open(catalog)
        self._skipped = 0
        self._emitted = 0

    def next(self) -> Optional[pa.RecordBatch]:
        if self._emitted >= self.n:
            return None
        while True:
            batch = self.child.next()
            if batch is None:
                return None

            # Handle offset
            if self._skipped < self.offset:
                remaining_skip = self.offset - self._skipped
                if batch.num_rows <= remaining_skip:
                    self._skipped += batch.num_rows
                    continue
                batch = batch.slice(remaining_skip)
                self._skipped = self.offset

            remaining = self.n - self._emitted
            if batch.num_rows > remaining:
                batch = batch.slice(0, remaining)
            self._emitted += batch.num_rows
            return batch

    def close(self) -> None:
        self.child.close()


# ---------------------------------------------------------------------------
# Hash Join  (build smaller right side, probe with left batches)
# ---------------------------------------------------------------------------

class HashJoinOp(PhysicalOp):
    """
    Classic hash join:
      Build phase: materialize right side into a hash table keyed on join column(s).
      Probe phase: for each left batch, look up matching right rows.
    """

    def __init__(
        self,
        left: PhysicalOp,
        right: PhysicalOp,
        left_keys: List[str],
        right_keys: List[str],
        join_type: str = "INNER",
    ) -> None:
        self.left = left
        self.right = right
        self.left_keys = left_keys
        self.right_keys = right_keys
        self.join_type = join_type
        self._hash_table: Dict[Tuple, List[Dict]] = {}
        self._built = False

    def open(self, catalog: Catalog) -> None:
        self.left.open(catalog)
        self.right.open(catalog)
        self._built = False
        self._hash_table = {}

    def _build(self) -> None:
        """Build hash table from entire right side."""
        ht: Dict[Tuple, List[Dict]] = collections.defaultdict(list)
        batch = self.right.next()
        while batch is not None:
            if batch.num_rows > 0:
                keys = [batch.column(k).to_pylist() for k in self.right_keys]
                for i in range(batch.num_rows):
                    key = tuple(keys[j][i] for j in range(len(self.right_keys)))
                    row = {name: batch.column(name)[i].as_py() for name in batch.schema.names}
                    ht[key].append(row)
            batch = self.right.next()
        self._hash_table = ht
        self._built = True

    def next(self) -> Optional[pa.RecordBatch]:
        if not self._built:
            self._build()

        while True:
            batch = self.left.next()
            if batch is None:
                return None
            if batch.num_rows == 0:
                continue

            left_keys = [batch.column(k).to_pylist() for k in self.left_keys]
            out_left: Dict[str, List] = {n: [] for n in batch.schema.names}
            out_right: Dict[str, List] = {}

            # Collect right column names
            if self._hash_table:
                sample = next(iter(self._hash_table.values()))[0]
                for rk in sample:
                    if rk not in out_left:
                        out_right[rk] = []

            matched_any = False
            for i in range(batch.num_rows):
                key = tuple(left_keys[j][i] for j in range(len(self.left_keys)))
                right_rows = self._hash_table.get(key, [])
                if right_rows:
                    for rrow in right_rows:
                        matched_any = True
                        for col in batch.schema.names:
                            out_left[col].append(batch.column(col)[i].as_py())
                        for rk, rv_list in out_right.items():
                            rv_list.append(rrow.get(rk))
                elif self.join_type == "LEFT":
                    matched_any = True
                    for col in batch.schema.names:
                        out_left[col].append(batch.column(col)[i].as_py())
                    for rk in out_right:
                        out_right[rk].append(None)

            if not matched_any:
                continue

            all_names = list(out_left) + list(out_right)
            all_arrays = [pa.array(out_left[n]) for n in out_left] + [
                pa.array(out_right[n]) for n in out_right
            ]
            return pa.RecordBatch.from_arrays(all_arrays, names=all_names)

    def close(self) -> None:
        self.left.close()
        self.right.close()
