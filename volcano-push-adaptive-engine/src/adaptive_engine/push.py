"""Push-based (pipeline) execution model.

Data flows *downward*: a source pumps rows into a chain of transforms,
each of which calls its downstream consumer.  This eliminates per-row
virtual dispatch overhead and dramatically improves cache locality for
high-cardinality hot paths.

Architecture:

    Source -> [Transform, ...] -> Sink

All stages are registered on a Pipeline object; calling pipeline.run()
drives the source to completion.
"""
from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

from .catalog import Catalog
from .expressions import Expr, Row
from .plan import (
    AggregateNode,
    FilterNode,
    HashJoinNode,
    LimitNode,
    PlanNode,
    ProjectNode,
    ScanNode,
    SortNode,
)


Consumer = Callable[[Row], None]


# ------------------------------------------------------------------
# Pipeline
# ------------------------------------------------------------------

class Pipeline:
    """A linear chain of push operators ending in a collecting sink."""

    def __init__(self) -> None:
        self._stages: list["PushOperator"] = []
        self._output: list[Row] = []

    def add(self, stage: "PushOperator") -> "Pipeline":
        self._stages.append(stage)
        return self

    def run(self) -> list[Row]:
        """Wire up the chain and drive the source."""
        self._output.clear()
        if not self._stages:
            return []

        # Wrap chain: each stage's consumer is the next stage's push
        consumers: list[Consumer] = [self._output.append]  # terminal sink

        for stage in reversed(self._stages[1:]):
            stage.set_consumer(consumers[0])
            consumers.insert(0, stage.push)

        source = self._stages[0]
        source.set_consumer(consumers[0])

        # Open all stages so hash joins can build their tables etc.
        for stage in self._stages:
            stage.open()

        # Source drives the forward data flow
        source.drain()

        # Flush blocking operators (aggregate, sort) that buffer before emitting
        for stage in self._stages[1:]:
            stage.drain()

        return list(self._output)


# ------------------------------------------------------------------
# Base push operator
# ------------------------------------------------------------------

class PushOperator:
    def __init__(self) -> None:
        self._consumer: Consumer | None = None

    def set_consumer(self, consumer: Consumer) -> None:
        self._consumer = consumer

    def open(self) -> None:
        """Called once before the first row."""

    def push(self, row: Row) -> None:
        """Receive a row from upstream; process and forward downstream."""
        if self._consumer:
            self._consumer(row)

    def drain(self) -> None:
        """Only meaningful for source operators: pump all rows."""

    def close(self) -> None:
        """Called once after the last row."""


# ------------------------------------------------------------------
# Source operators
# ------------------------------------------------------------------

class ScanPush(PushOperator):
    def __init__(self, catalog: Catalog, table: str) -> None:
        super().__init__()
        self._catalog = catalog
        self._table = table

    def drain(self) -> None:
        assert self._consumer
        for row in self._catalog.data(self._table):
            self._consumer(dict(row))


class RowBufferPush(PushOperator):
    """Source from a pre-materialised list of rows."""

    def __init__(self, rows: list[Row]) -> None:
        super().__init__()
        self._rows = rows

    def drain(self) -> None:
        assert self._consumer
        for row in self._rows:
            self._consumer(row)


# ------------------------------------------------------------------
# Transform operators
# ------------------------------------------------------------------

class FilterPush(PushOperator):
    def __init__(self, predicate: Expr) -> None:
        super().__init__()
        self._pred = predicate

    def push(self, row: Row) -> None:
        if self._pred.eval(row):
            assert self._consumer
            self._consumer(row)


class ProjectPush(PushOperator):
    def __init__(self, columns: list[str]) -> None:
        super().__init__()
        self._cols = columns

    def push(self, row: Row) -> None:
        assert self._consumer
        self._consumer({c: row[c] for c in self._cols if c in row})


class HashJoinPush(PushOperator):
    """Two-phase: build side is fully materialised, then probe side is pushed."""

    def __init__(
        self,
        catalog: Catalog,
        build_plan: PlanNode,
        build_key: str,
        probe_key: str,
        join_type: str = "inner",
    ) -> None:
        super().__init__()
        self._catalog = catalog
        self._build_plan = build_plan
        self._build_key = build_key
        self._probe_key = probe_key
        self._join_type = join_type
        self._ht: dict[Any, list[Row]] = defaultdict(list)

    def open(self) -> None:
        # Materialise build side using volcano (it may itself be a push pipeline)
        from .volcano import VolcanoExecutor
        exec_ = VolcanoExecutor(self._catalog)
        for row in exec_.iter(self._build_plan):
            self._ht[row.get(self._build_key)].append(row)

    def push(self, row: Row) -> None:
        assert self._consumer
        key = row.get(self._probe_key)
        matches = self._ht.get(key, [])
        if matches:
            for build_row in matches:
                self._consumer({**row, **build_row})
        elif self._join_type in ("left", "full"):
            self._consumer(dict(row))


class AggregatePush(PushOperator):
    """Streaming group-by aggregation; emits all groups on close."""

    def __init__(
        self,
        group_by: list[str],
        aggregates: list[tuple[str, str, str]],
    ) -> None:
        super().__init__()
        self._group_by = group_by
        self._aggregates = aggregates
        self._groups: dict[tuple, dict] = {}

    def push(self, row: Row) -> None:
        key = tuple(row.get(c) for c in self._group_by)
        if key not in self._groups:
            self._groups[key] = {c: row.get(c) for c in self._group_by}
            for out_col, func, _ in self._aggregates:
                self._groups[key][out_col] = _agg_init(func)
        for out_col, func, in_col in self._aggregates:
            val = 1 if in_col == "*" else row.get(in_col)
            self._groups[key][out_col] = _agg_step(func, self._groups[key][out_col], val)

    def close(self) -> None:
        assert self._consumer
        for acc in self._groups.values():
            result = dict(acc)
            for out_col, func, _ in self._aggregates:
                result[out_col] = _agg_final(func, result[out_col])
            self._consumer(result)
        self._groups.clear()

    def drain(self) -> None:
        self.close()


class SortPush(PushOperator):
    """Blocking sort; emits sorted rows on drain/close."""

    def __init__(self, order_by: list[tuple[str, bool]]) -> None:
        super().__init__()
        self._order_by = order_by
        self._buf: list[Row] = []

    def push(self, row: Row) -> None:
        self._buf.append(row)

    def close(self) -> None:
        assert self._consumer
        rows = self._buf
        for col, ascending in reversed(self._order_by):
            rows.sort(key=lambda r: (r.get(col) is None, r.get(col)), reverse=not ascending)
        for row in rows:
            self._consumer(row)
        self._buf = []

    def drain(self) -> None:
        self.close()


class LimitPush(PushOperator):
    def __init__(self, limit: int, offset: int = 0) -> None:
        super().__init__()
        self._limit = limit
        self._offset = offset
        self._seen = 0
        self._emitted = 0

    def open(self) -> None:
        self._seen = 0
        self._emitted = 0

    def push(self, row: Row) -> None:
        if self._seen < self._offset:
            self._seen += 1
            return
        if self._emitted >= self._limit:
            return
        assert self._consumer
        self._consumer(row)
        self._seen += 1
        self._emitted += 1


# ------------------------------------------------------------------
# Compiler: PlanNode -> Pipeline
# ------------------------------------------------------------------

class PushCompiler:
    """Compiles a plan subtree into a push Pipeline.

    Only linear plans (no branches) can be compiled directly; joins
    handle their build side internally via HashJoinPush.open().
    """

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    def compile(self, root: PlanNode) -> Pipeline:
        pipeline = Pipeline()
        stages = self._collect_stages(root)
        for stage in stages:
            pipeline.add(stage)
        return pipeline

    def _collect_stages(self, node: PlanNode) -> list[PushOperator]:
        match node:
            case ScanNode():
                return [ScanPush(self.catalog, node.table)]

            case FilterNode():
                assert node.child and node.predicate
                stages = self._collect_stages(node.child)
                stages.append(FilterPush(node.predicate))
                return stages

            case ProjectNode():
                assert node.child
                stages = self._collect_stages(node.child)
                stages.append(ProjectPush(node.columns))
                return stages

            case HashJoinNode():
                assert node.left and node.right
                probe_stages = self._collect_stages(node.left)
                join_op = HashJoinPush(
                    self.catalog,
                    node.right,
                    node.right_key,
                    node.left_key,
                    node.join_type,
                )
                probe_stages.append(join_op)
                return probe_stages

            case AggregateNode():
                assert node.child
                stages = self._collect_stages(node.child)
                stages.append(AggregatePush(node.group_by, node.aggregates))
                return stages

            case SortNode():
                assert node.child
                stages = self._collect_stages(node.child)
                stages.append(SortPush(node.order_by))
                return stages

            case LimitNode():
                assert node.child
                stages = self._collect_stages(node.child)
                stages.append(LimitPush(node.limit, node.offset))
                return stages

            case _:
                raise NotImplementedError(
                    f"PushCompiler: no stage for {type(node).__name__}"
                )


# ------------------------------------------------------------------
# Aggregate helpers (shared with volcano.py to avoid circular import)
# ------------------------------------------------------------------

from typing import Any


def _agg_init(func: str) -> Any:
    return {"count": 0, "sum": 0, "avg": (0, 0), "min": None, "max": None}[func]


def _agg_step(func: str, acc: Any, val: Any) -> Any:
    if val is None:
        return acc
    match func:
        case "count":
            return acc + 1
        case "sum":
            return acc + val
        case "avg":
            total, n = acc
            return (total + val, n + 1)
        case "min":
            return val if acc is None else min(acc, val)
        case "max":
            return val if acc is None else max(acc, val)
    return acc


def _agg_final(func: str, acc: Any) -> Any:
    if func == "avg":
        total, n = acc
        return total / n if n > 0 else None
    return acc
