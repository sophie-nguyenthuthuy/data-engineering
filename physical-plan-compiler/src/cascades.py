"""Cascades-style cost-based planner with memoization over physical groups.

For each logical operator we enumerate physical alternatives across all
engines, propagate physical properties (output engine), and insert
conversions where parent's required input engine differs from child's
output engine. Search picks the minimum-cost plan via memoization.
"""
from __future__ import annotations

from dataclasses import dataclass

from .logical import LogicalOp, Source, Filter, Aggregate, Join
from .physical import (
    PhysicalOp, ENGINE_COSTS, conversion_cost,
    filter_cost, aggregate_cost, join_cost,
)


@dataclass
class PlannedNode:
    op: PhysicalOp
    total_cost: float
    output_bytes: float


def plan(logical: LogicalOp, target_engines: list[str] = None) -> PlannedNode:
    """Search for the minimum-cost physical plan."""
    target_engines = target_engines or ["spark", "dbt", "duckdb", "flink"]
    memo = {}

    def best(op: LogicalOp, output_engine: str) -> PlannedNode:
        key = (id(op), output_engine)
        if key in memo:
            return memo[key]

        candidates: list[PlannedNode] = []

        if isinstance(op, Source):
            bytes_out = op.estimated_rows * 100
            # Source can be read from any engine
            for eng in target_engines:
                cost = ENGINE_COSTS[eng]["setup"] + bytes_out * ENGINE_COSTS[eng]["per_byte"] * 0.1
                node = PhysicalOp(
                    kind="scan", engine=eng, cost=cost, bytes_out=bytes_out,
                    output_engine=eng,
                )
                conv = conversion_cost(eng, output_engine)
                candidates.append(PlannedNode(
                    op=node, total_cost=cost + conv, output_bytes=bytes_out,
                ))

        elif isinstance(op, Filter):
            for eng in target_engines:
                child = best(op.children[0], eng)
                fcost, fout = filter_cost(eng, child.output_bytes, op.selectivity)
                node = PhysicalOp(
                    kind="filter", engine=eng, cost=fcost, bytes_out=fout,
                    children=[child.op], output_engine=eng,
                )
                conv = conversion_cost(eng, output_engine)
                candidates.append(PlannedNode(
                    op=node, total_cost=child.total_cost + fcost + conv,
                    output_bytes=fout,
                ))

        elif isinstance(op, Aggregate):
            for eng in target_engines:
                child = best(op.children[0], eng)
                acost, aout = aggregate_cost(eng, child.output_bytes, group_card=1000)
                node = PhysicalOp(
                    kind="aggregate", engine=eng, cost=acost, bytes_out=aout,
                    children=[child.op], output_engine=eng,
                )
                conv = conversion_cost(eng, output_engine)
                candidates.append(PlannedNode(
                    op=node, total_cost=child.total_cost + acost + conv,
                    output_bytes=aout,
                ))

        elif isinstance(op, Join):
            for eng in target_engines:
                l = best(op.children[0], eng)
                r = best(op.children[1], eng)
                jcost, jout = join_cost(eng, l.output_bytes, r.output_bytes)
                node = PhysicalOp(
                    kind="join", engine=eng, cost=jcost, bytes_out=jout,
                    children=[l.op, r.op], output_engine=eng,
                )
                conv = conversion_cost(eng, output_engine)
                candidates.append(PlannedNode(
                    op=node, total_cost=l.total_cost + r.total_cost + jcost + conv,
                    output_bytes=jout,
                ))

        # Pruning: keep only the min-cost
        best_plan = min(candidates, key=lambda c: c.total_cost)
        memo[key] = best_plan
        return best_plan

    # No constraint on outer engine — try all
    final_candidates = []
    for eng in target_engines:
        final_candidates.append(best(logical, eng))
    return min(final_candidates, key=lambda c: c.total_cost)


__all__ = ["PlannedNode", "plan"]
