"""
Top-level Engine: register tables, execute SQL, choose execution model.

Usage:
    engine = Engine()
    engine.register("lineitem", arrow_table)
    result = engine.execute("SELECT SUM(l_quantity) FROM lineitem WHERE l_discount > 0.05")
"""
from __future__ import annotations

from typing import Literal, Optional

import pyarrow as pa
import pyarrow.compute as pc

from .catalog import Catalog
from .expressions import ColumnRef
from .logical_plan import (
    Aggregate,
    Filter,
    Join,
    Limit,
    LogicalPlan,
    Project,
    Scan,
    Sort,
)
from .optimizer import Optimizer
from .parser import parse
from .physical_plan import (
    BATCH_SIZE,
    FilterOp,
    HashAggOp,
    HashJoinOp,
    LimitOp,
    PhysicalOp,
    ProjectOp,
    SequentialScan,
    SortOp,
)
from .pipeline import (
    CollectSink,
    HashAggSink,
    Pipeline,
    PipelineExecutor,
    PushFilter,
    PushLimit,
    PushProject,
    SortSink,
)
from .planner import Planner


ExecutionMode = Literal["volcano", "pipeline"]


class Engine:
    """
    Vectorized columnar query engine.

    Supports two execution models:
      - "volcano"  : pull-based iterator model (classic Volcano/Cascades)
      - "pipeline" : push-based pipeline model (Hyper/DuckDB style)

    Both models use:
      - Apache Arrow columnar batches (SIMD-friendly layout)
      - Predicate pushdown (filters evaluated at scan time)
      - Late materialization (predicate columns read first; other cols fetched after filter)
      - Projection pushdown (only needed columns read from table)
      - Hash aggregation
      - Hash join (equi-join only)
    """

    def __init__(self) -> None:
        self.catalog = Catalog()
        self._optimizer = Optimizer()
        self._planner = Planner()

    # ------------------------------------------------------------------
    # Table registration
    # ------------------------------------------------------------------

    def register(self, name: str, data: pa.Table) -> None:
        self.catalog.register(name, data)

    def register_dict(self, name: str, d: dict) -> None:
        self.catalog.register(name, pa.table(d))

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(
        self,
        sql: str,
        mode: ExecutionMode = "pipeline",
        optimize: bool = True,
    ) -> pa.Table:
        logical = parse(sql)
        if optimize:
            logical = self._optimizer.optimize(logical)

        if mode == "volcano":
            return self._execute_volcano(logical)
        return self._execute_pipeline(logical)

    def explain(self, sql: str, optimize: bool = True) -> str:
        logical = parse(sql)
        lines = ["=== Logical Plan (before optimization) ==="]
        lines.append(logical.pretty())
        if optimize:
            optimized = self._optimizer.optimize(logical)
            lines.append("\n=== Logical Plan (after optimization) ===")
            lines.append(optimized.pretty())
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Volcano executor
    # ------------------------------------------------------------------

    def _execute_volcano(self, plan: LogicalPlan) -> pa.Table:
        op = self._planner.plan(plan)
        return op.collect(self.catalog)

    # ------------------------------------------------------------------
    # Pipeline executor
    # ------------------------------------------------------------------

    def _execute_pipeline(self, plan: LogicalPlan) -> pa.Table:
        pipelines = self._build_pipelines(plan)
        executor = PipelineExecutor()
        return executor.execute(pipelines, self.catalog)

    def _build_pipelines(self, plan: LogicalPlan) -> list[Pipeline]:
        """
        Walk the logical plan and build a list of Pipeline objects.
        Pipeline-breakers (Aggregate, Sort) end one pipeline and start the next.
        The result of a breaker is fed into downstream operators via a CollectSink
        feeding a subsequent pipeline (simplified; full morsel-driven would use
        shared hash tables between pipelines).
        """
        # For simplicity, fall back to volcano for complex multi-join plans
        # and use pure pipeline for single-scan plans.
        if _has_join(plan):
            # Build a single pipeline by wrapping the volcano plan
            return [self._volcano_as_pipeline(plan)]

        return [self._plan_to_pipeline(plan)]

    def _plan_to_pipeline(self, plan: LogicalPlan) -> Pipeline:
        source, ops, sink = self._decompose(plan)
        pipeline = Pipeline(source=source, ops=ops, sink=sink)
        return pipeline

    def _decompose(self, plan: LogicalPlan):
        """Recursively decompose plan into (scan, [push_ops], sink)."""
        if isinstance(plan, Scan):
            scan = SequentialScan(plan.table, plan.columns, plan.pushed_predicates)
            return scan, [], CollectSink()

        if isinstance(plan, Filter):
            scan, ops, sink = self._decompose(plan.child)
            ops = ops + [PushFilter(plan.predicate)]
            return scan, ops, sink

        if isinstance(plan, Project):
            child = plan.child
            # If projecting over a breaker (agg/sort), apply the project post-finish
            if isinstance(child, (Aggregate, Sort)):
                scan, ops, sink = self._decompose(child)
                wrapped = _PostProjectSink(sink, plan.exprs, plan.aliases)
                return scan, ops, wrapped
            scan, ops, sink = self._decompose(child)
            ops = ops + [PushProject(plan.exprs, plan.aliases)]
            return scan, ops, sink

        if isinstance(plan, Limit):
            scan, ops, sink = self._decompose(plan.child)
            ops = ops + [PushLimit(plan.n)]
            return scan, ops, sink

        if isinstance(plan, Aggregate):
            scan, ops, _ = self._decompose(plan.child)
            sink = HashAggSink(plan.group_by, plan.aggregates)
            return scan, ops, sink

        if isinstance(plan, Sort):
            scan, ops, inner_sink = self._decompose(plan.child)
            # Wrap whatever inner_sink we have with a post-sort step
            wrapped = _PostSortSink(inner_sink, plan.keys, plan.ascending)
            return scan, ops, wrapped

        raise NotImplementedError(f"Cannot build pipeline for {type(plan).__name__}")

    def _volcano_as_pipeline(self, plan: LogicalPlan) -> Pipeline:
        """Wrap a full volcano plan as a single pipeline that collects results."""
        op = self._planner.plan(plan)

        class _VolcanoScan:
            def __init__(self, physical_op):
                self._op = physical_op
                self._opened = False

            def open(self, catalog):
                self._op.open(catalog)

            def next(self):
                return self._op.next()

            def close(self):
                self._op.close()

        sink = CollectSink()
        return Pipeline(source=_VolcanoScan(op), ops=[], sink=sink)


def _has_join(plan: LogicalPlan) -> bool:
    if isinstance(plan, Join):
        return True
    for child in plan.children():
        if _has_join(child):
            return True
    return False


class _PostSortSink:
    """
    Wraps any sink and sorts its result after finish().
    Composes cleanly with _PostProjectSink and HashAggSink.
    """

    def __init__(self, inner, keys, ascending) -> None:
        self._inner = inner
        self._keys = keys
        self._ascending = ascending
        self._result = None

    def set_downstream(self, ds) -> None:
        pass

    def push(self, batch) -> None:
        self._inner.push(batch)

    def finish(self) -> None:
        self._inner.finish()
        tbl = self._inner.result()
        if tbl is None or tbl.num_rows == 0:
            self._result = tbl
            return
        sort_keys = []
        for k, asc in zip(self._keys, self._ascending):
            name = k.name if isinstance(k, ColumnRef) else repr(k)
            if name in tbl.schema.names:
                sort_keys.append((name, "ascending" if asc else "descending"))
        if sort_keys:
            indices = pc.sort_indices(tbl, sort_keys=sort_keys)
            tbl = tbl.take(indices)
        self._result = tbl

    def result(self):
        return self._result


class _PostProjectSink:
    """
    Wraps any sink and applies a projection to its result after finish().
    Used when a Project node sits directly above a pipeline-breaker (Agg, Sort).
    """

    def __init__(self, inner, exprs, aliases) -> None:
        self._inner = inner
        self._exprs = exprs
        self._aliases = aliases
        self._result = None

    def set_downstream(self, ds) -> None:
        pass

    def push(self, batch) -> None:
        self._inner.push(batch)

    def finish(self) -> None:
        self._inner.finish()
        tbl = self._inner.result()
        if tbl is None or tbl.num_rows == 0:
            self._result = tbl
            return
        # Apply projection to the materialized result
        schema = tbl.schema
        out_names = []
        out_arrays = []
        for i, expr in enumerate(self._exprs):
            alias = self._aliases[i] if i < len(self._aliases) else None
            if isinstance(expr, ColumnRef) and expr.name in schema.names:
                col = tbl.column(expr.name)
                name = alias or expr.name
            else:
                # Evaluate expression against the full table as batches
                batches = tbl.to_batches(max_chunksize=8192)
                parts = [expr.eval(b) for b in batches]
                col = pa.chunked_array(parts) if parts else pa.array([])
                name = alias or repr(expr)
            out_names.append(name)
            out_arrays.append(col)
        self._result = pa.table(dict(zip(out_names, out_arrays)))

    def result(self):
        return self._result
