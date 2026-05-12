"""Pull-based (Volcano/iterator) execution model.

Each operator is an iterator that yields rows on demand.
The root operator drives everything top-down; data flows bottom-up.
"""
from __future__ import annotations
from collections import defaultdict
from typing import Iterator

from .catalog import Catalog
from .expressions import Row
from .plan import (
    AggregateNode,
    BufferNode,
    FilterNode,
    HashJoinNode,
    LimitNode,
    NestedLoopJoinNode,
    PlanNode,
    ProjectNode,
    ScanNode,
    SortNode,
)


class VolcanoExecutor:
    """Compiles a plan tree into a nested iterator and drives it to completion."""

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    def execute(self, plan: PlanNode) -> list[Row]:
        return list(self._iter(plan))

    def iter(self, plan: PlanNode) -> Iterator[Row]:
        return self._iter(plan)

    # ------------------------------------------------------------------
    # Iterator compilation
    # ------------------------------------------------------------------

    def _iter(self, node: PlanNode) -> Iterator[Row]:
        match node:
            case ScanNode():
                yield from self._scan(node)
            case FilterNode():
                yield from self._filter(node)
            case ProjectNode():
                yield from self._project(node)
            case HashJoinNode():
                yield from self._hash_join(node)
            case NestedLoopJoinNode():
                yield from self._nl_join(node)
            case AggregateNode():
                yield from self._aggregate(node)
            case SortNode():
                yield from self._sort(node)
            case LimitNode():
                yield from self._limit(node)
            case BufferNode():
                yield from node.rows
            case _:
                raise NotImplementedError(f"No volcano executor for {type(node).__name__}")

    # ------------------------------------------------------------------
    # Operator implementations
    # ------------------------------------------------------------------

    def _scan(self, node: ScanNode) -> Iterator[Row]:
        for row in self.catalog.data(node.table):
            yield dict(row)

    def _filter(self, node: FilterNode) -> Iterator[Row]:
        assert node.child and node.predicate
        for row in self._iter(node.child):
            if node.predicate.eval(row):
                yield row

    def _project(self, node: ProjectNode) -> Iterator[Row]:
        assert node.child
        cols = node.columns
        for row in self._iter(node.child):
            yield {c: row[c] for c in cols if c in row}

    def _hash_join(self, node: HashJoinNode) -> Iterator[Row]:
        assert node.left and node.right

        # Build phase: materialise the build side (right) into a hash table
        build_ht: dict[Any, list[Row]] = defaultdict(list)
        for row in self._iter(node.right):
            key = row.get(node.right_key)
            build_ht[key].append(row)

        # Probe phase: stream the probe side (left) and look up matches
        join_type = node.join_type
        for probe_row in self._iter(node.left):
            key = probe_row.get(node.left_key)
            matches = build_ht.get(key, [])
            if matches:
                for build_row in matches:
                    yield {**probe_row, **build_row}
            elif join_type in ("left", "full"):
                # Emit probe row with NULLs for build columns
                yield dict(probe_row)

    def _nl_join(self, node: NestedLoopJoinNode) -> Iterator[Row]:
        assert node.left and node.right and node.predicate
        right_rows = list(self._iter(node.right))
        for left_row in self._iter(node.left):
            for right_row in right_rows:
                merged = {**left_row, **right_row}
                if node.predicate.eval(merged):
                    yield merged

    def _aggregate(self, node: AggregateNode) -> Iterator[Row]:
        assert node.child
        groups: dict[tuple, dict] = {}

        for row in self._iter(node.child):
            group_key = tuple(row.get(c) for c in node.group_by)
            if group_key not in groups:
                groups[group_key] = {
                    col: row.get(col) for col in node.group_by
                }
                for out_col, func, _ in node.aggregates:
                    groups[group_key][out_col] = _agg_init(func)
            for out_col, func, in_col in node.aggregates:
                val = 1 if in_col == "*" else row.get(in_col)
                groups[group_key][out_col] = _agg_step(func, groups[group_key][out_col], val)

        for group_key, acc in groups.items():
            result = dict(acc)
            for out_col, func, _ in node.aggregates:
                result[out_col] = _agg_final(func, result[out_col])
            yield result

    def _sort(self, node: SortNode) -> Iterator[Row]:
        assert node.child
        rows = list(self._iter(node.child))
        for col, ascending in reversed(node.order_by):
            rows.sort(key=lambda r: (r.get(col) is None, r.get(col)), reverse=not ascending)
        yield from rows

    def _limit(self, node: LimitNode) -> Iterator[Row]:
        assert node.child
        count = 0
        skipped = 0
        for row in self._iter(node.child):
            if skipped < node.offset:
                skipped += 1
                continue
            if count >= node.limit:
                break
            yield row
            count += 1


# ------------------------------------------------------------------
# Aggregate helpers
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
