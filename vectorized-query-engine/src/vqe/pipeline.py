"""
Pipeline (push-based) execution model.

In the volcano/pull model, each operator calls next() on its child — the control
flow is top-down and each batch crosses N function call boundaries on the way up.

In the pipeline/push model, data is pushed bottom-up through a chain of operators
inside a tight loop.  Pipeline-breakers (aggregates, sorts) end one pipeline and
start the next.  This layout:
  - Eliminates per-batch virtual-dispatch overhead between pipelineable operators.
  - Improves cache locality: the entire chain for a batch runs before the batch
    is evicted from L1/L2.
  - Enables morsel-driven parallelism (each morsel is one pipeline invocation).

Implementation sketch:
  Pipeline := [source, op1, op2, ..., sink]
  Executor pulls batches from source and calls sink.push(batch) which chains
  through all intermediate ops inline.
"""
from __future__ import annotations

import collections
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Dict, Generator, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from .catalog import Catalog
from .expressions import AggExpr, ColumnRef, Expr
from .physical_plan import BATCH_SIZE, SequentialScan


# ---------------------------------------------------------------------------
# Push-based operator interface
# ---------------------------------------------------------------------------

class PushOp(ABC):
    """An operator that consumes batches and forwards them to a downstream sink."""

    def set_downstream(self, downstream: "PushOp") -> None:
        self._downstream = downstream

    @abstractmethod
    def push(self, batch: pa.RecordBatch) -> None: ...

    @abstractmethod
    def finish(self) -> None:
        """Signal end of input; flush any buffered state."""
        ...

    def result(self) -> Optional[pa.Table]:
        """For sink operators: return accumulated result."""
        return None


# ---------------------------------------------------------------------------
# Pipelineable (non-blocking) operators
# ---------------------------------------------------------------------------

class PushFilter(PushOp):
    def __init__(self, predicate: Expr) -> None:
        self.predicate = predicate
        self._downstream: Optional[PushOp] = None

    def push(self, batch: pa.RecordBatch) -> None:
        if batch.num_rows == 0:
            return
        mask = self.predicate.eval(batch)
        filtered = batch.filter(mask)
        if filtered.num_rows > 0 and self._downstream:
            self._downstream.push(filtered)

    def finish(self) -> None:
        if self._downstream:
            self._downstream.finish()


class PushProject(PushOp):
    def __init__(self, exprs: List[Expr], aliases: List[Optional[str]]) -> None:
        self.exprs = exprs
        self.aliases = aliases
        self._downstream: Optional[PushOp] = None

    def _name(self, i: int) -> str:
        a = self.aliases[i] if i < len(self.aliases) else None
        if a:
            return a
        e = self.exprs[i]
        return e.name if isinstance(e, ColumnRef) else repr(e)

    def push(self, batch: pa.RecordBatch) -> None:
        arrays = [e.eval(batch) for e in self.exprs]
        names = [self._name(i) for i in range(len(self.exprs))]
        out = pa.RecordBatch.from_arrays(arrays, names=names)
        if self._downstream:
            self._downstream.push(out)

    def finish(self) -> None:
        if self._downstream:
            self._downstream.finish()


class PushLimit(PushOp):
    def __init__(self, n: int) -> None:
        self.n = n
        self._emitted = 0
        self._downstream: Optional[PushOp] = None

    def push(self, batch: pa.RecordBatch) -> None:
        if self._emitted >= self.n:
            return
        remaining = self.n - self._emitted
        if batch.num_rows > remaining:
            batch = batch.slice(0, remaining)
        self._emitted += batch.num_rows
        if self._downstream:
            self._downstream.push(batch)

    def finish(self) -> None:
        if self._downstream:
            self._downstream.finish()


# ---------------------------------------------------------------------------
# Pipeline-breaker sinks
# ---------------------------------------------------------------------------

class CollectSink(PushOp):
    """Trivial sink: collect all batches into a list."""

    def __init__(self) -> None:
        self._batches: List[pa.RecordBatch] = []

    def push(self, batch: pa.RecordBatch) -> None:
        if batch.num_rows > 0:
            self._batches.append(batch)

    def finish(self) -> None:
        pass

    def result(self) -> Optional[pa.Table]:
        if not self._batches:
            return pa.table({})
        return pa.Table.from_batches(self._batches)


class HashAggSink(PushOp):
    """
    Pipeline-breaking aggregate sink.
    Accumulates partial states per group, finalizes when finish() is called.
    """

    def __init__(self, group_by: List[Expr], aggregates: List[AggExpr]) -> None:
        self.group_by = group_by
        self.aggregates = aggregates
        self._states: Dict[Tuple, List] = collections.defaultdict(
            lambda: [None] * len(aggregates)
        )
        self._result: Optional[pa.Table] = None

    def push(self, batch: pa.RecordBatch) -> None:
        n = batch.num_rows
        if n == 0:
            return

        if self.group_by:
            key_cols = [e.eval(batch).to_pylist() for e in self.group_by]
            group_indices: Dict[Tuple, List[int]] = collections.defaultdict(list)
            for i in range(n):
                key = tuple(col[i] for col in key_cols)
                group_indices[key].append(i)

            for key, indices in group_indices.items():
                sub = batch.take(pa.array(indices))
                for j, agg in enumerate(self.aggregates):
                    partial = agg.partial(sub)
                    self._states[key][j] = agg.merge(self._states[key][j], partial)
        else:
            key = ()
            for j, agg in enumerate(self.aggregates):
                partial = agg.partial(batch)
                self._states[key][j] = agg.merge(self._states[key][j], partial)

    def finish(self) -> None:
        key_arrays: List[List] = [[] for _ in self.group_by]
        agg_arrays: List[List] = [[] for _ in self.aggregates]

        for key, agg_states in self._states.items():
            for i, v in enumerate(key):
                key_arrays[i].append(v)
            for j, agg in enumerate(self.aggregates):
                agg_arrays[j].append(agg.finalize(agg_states[j]))

        names: List[str] = []
        arrays: List[pa.Array] = []
        for i, e in enumerate(self.group_by):
            names.append(e.name if isinstance(e, ColumnRef) else repr(e))
            arrays.append(pa.array(key_arrays[i]))
        for j, agg in enumerate(self.aggregates):
            names.append(agg.output_name)
            arrays.append(pa.array(agg_arrays[j]))

        if names:
            self._result = pa.table(dict(zip(names, arrays)))
        else:
            self._result = pa.table({})

    def result(self) -> Optional[pa.Table]:
        return self._result


class SortSink(PushOp):
    def __init__(self, keys: List[Expr], ascending: List[bool]) -> None:
        self.keys = keys
        self.ascending = ascending
        self._batches: List[pa.RecordBatch] = []
        self._result: Optional[pa.Table] = None

    def push(self, batch: pa.RecordBatch) -> None:
        if batch.num_rows > 0:
            self._batches.append(batch)

    def finish(self) -> None:
        if not self._batches:
            self._result = pa.table({})
            return
        tbl = pa.Table.from_batches(self._batches)
        sort_keys = []
        for k, asc in zip(self.keys, self.ascending):
            name = k.name if isinstance(k, ColumnRef) else repr(k)
            if name in tbl.schema.names:
                sort_keys.append((name, "ascending" if asc else "descending"))
        if sort_keys:
            indices = pc.sort_indices(tbl, sort_keys=sort_keys)
            tbl = tbl.take(indices)
        self._result = tbl

    def result(self) -> Optional[pa.Table]:
        return self._result


# ---------------------------------------------------------------------------
# Pipeline and executor
# ---------------------------------------------------------------------------

@dataclass
class Pipeline:
    """A linear chain: source → [ops] → sink."""
    source: SequentialScan
    ops: List[PushOp]        # filter, project, limit — non-blocking
    sink: PushOp             # collect, agg, sort — may block

    def wire(self) -> None:
        """Connect operators in order."""
        chain: List[PushOp] = self.ops + [self.sink]
        for i in range(len(chain) - 1):
            chain[i].set_downstream(chain[i + 1])

    def execute(self, catalog: Catalog) -> pa.Table:
        self.wire()
        self.source.open(catalog)
        while True:
            batch = self.source.next()
            if batch is None:
                break
            # Push through the chain
            if self.ops:
                self.ops[0].push(batch)
            else:
                self.sink.push(batch)
        self.source.close()
        self.sink.finish()
        return self.sink.result() or pa.table({})


class PipelineExecutor:
    """
    Executes a sequence of pipelines where each pipeline's result feeds
    the next (for multi-pipeline plans like sort → limit).
    """

    def execute(self, pipelines: List[Pipeline], catalog: Catalog) -> pa.Table:
        result = None
        for pipeline in pipelines:
            result = pipeline.execute(catalog)
        return result or pa.table({})
