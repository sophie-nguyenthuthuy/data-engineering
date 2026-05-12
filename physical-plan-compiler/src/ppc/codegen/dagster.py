"""Dagster orchestration manifest: one asset per engine sub-plan.

We walk the physical plan and split it at PhysicalConversion boundaries.
Each contiguous engine-region becomes one Dagster asset. The conversions
become explicit `materialise → register external` steps.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ppc.codegen.dbt_codegen import emit_dbt
from ppc.codegen.duckdb_codegen import emit_duckdb
from ppc.codegen.flink_codegen import emit_flink
from ppc.codegen.spark_codegen import emit_spark
from ppc.engines.physical_ops import PhysicalConversion
from ppc.ir.physical import PhysicalNode, PhysicalPlan


@dataclass
class _Asset:
    asset_id: str
    engine: str
    body: str
    depends_on: list[str] = field(default_factory=list)


_CODEGENS = {
    "spark":  emit_spark,
    "dbt":    emit_dbt,
    "duckdb": emit_duckdb,
    "flink":  emit_flink,
}


def emit_dagster(plan: PhysicalPlan) -> dict:
    """Return a dict suitable for YAML dump."""
    assets: list[_Asset] = []
    counter = {"n": 0}

    def _walk(node: PhysicalNode) -> str:
        """Walks the plan, emits assets for each engine region. Returns id."""
        if isinstance(node, PhysicalConversion):
            inner = _walk(node.child)
            counter["n"] += 1
            aid = f"conv_{counter['n']:02d}_{node.src_engine}_to_{node.dst_engine}"
            assets.append(_Asset(
                asset_id=aid,
                engine="conversion",
                body=f"# materialise {node.src_engine} result -> {node.dst_engine}-readable\n"
                     f"# bytes≈{node.bytes_in:,.0f}",
                depends_on=[inner],
            ))
            return aid
        # Non-conversion: this is the root of an engine region.
        counter["n"] += 1
        aid = f"op_{counter['n']:02d}_{node.engine}_{node.kind}"
        # We need to know child IDs that come *from other engines* — those
        # are the conversions we descended through. We just emit the whole
        # subtree's code for this asset (PPC emits self-contained code per
        # region).
        sub_plan = PhysicalPlan(
            root=node, total_cost=node.cost,
            estimated_bytes=node.bytes_out, logical=plan.logical,
        )
        body = _CODEGENS[node.engine](sub_plan)
        # Recurse into children to collect upstream IDs
        deps: list[str] = []
        for c in node.children:
            deps.append(_walk(c))
        assets.append(_Asset(asset_id=aid, engine=node.engine, body=body, depends_on=deps))
        return aid

    root_id = _walk(plan.root)
    return {
        "version": "1.0",
        "pipeline": "ppc-generated",
        "estimated_cost": plan.total_cost,
        "root_asset": root_id,
        "assets": [
            {
                "asset_id": a.asset_id,
                "engine": a.engine,
                "depends_on": a.depends_on,
                "body": a.body,
            }
            for a in assets
        ],
    }
