"""Algebraic DAG rewrites applied before code generation."""
from __future__ import annotations

import copy
from dataclasses import replace

from ..dsl.ir import PipelineSpec, TransformNode
from ..dsl.types import TransformType


def optimize(spec: PipelineSpec) -> PipelineSpec:
    """Apply all optimization passes, returning a new PipelineSpec."""
    spec = copy.deepcopy(spec)
    # Eliminate identity SELECTs first so push-down sees the simplified graph
    spec = _eliminate_identity_selects(spec)
    spec = _merge_consecutive_selects(spec)
    spec = _push_filters_down(spec)
    spec.infer_schemas()
    return spec


# ──────────────────────────────────────────────────────────────
# Pass 1 – filter push-down
# ──────────────────────────────────────────────────────────────

def _push_filters_down(spec: PipelineSpec) -> PipelineSpec:
    """
    Move FILTER nodes as close to their source as possible.
    Pattern: if a FILTER's sole input is a SELECT, and the filter predicate
    only references columns that exist before the SELECT, we can swap them.
    """
    changed = True
    while changed:
        changed = False
        topo = spec.topological_order()
        for node in topo:
            if node.transform_type != TransformType.FILTER:
                continue
            if len(node.inputs) != 1:
                continue
            parent_name = node.inputs[0]
            parent = spec.nodes[parent_name]
            if parent.transform_type != TransformType.SELECT:
                continue

            # Only swap if filter predicate columns are available before the select
            pred_cols = _columns_in_predicate(node.predicate or "")
            pre_select_cols = set(
                f.name for inp in parent.inputs
                if inp in spec.nodes and spec.nodes[inp].output_schema
                for f in spec.nodes[inp].output_schema.fields
            )
            if pred_cols <= pre_select_cols:
                # swap: filter goes before select
                new_filter = TransformNode(
                    name=node.name + "_pushed",
                    transform_type=TransformType.FILTER,
                    inputs=parent.inputs[:],
                    predicate=node.predicate,
                )
                new_select = TransformNode(
                    name=node.name,
                    transform_type=TransformType.SELECT,
                    inputs=[new_filter.name],
                    columns=parent.columns,
                )
                del spec.nodes[node.name]
                del spec.nodes[parent_name]
                spec.nodes[new_filter.name] = new_filter
                spec.nodes[new_select.name] = new_select
                # Re-wire anything that used to point at the old filter
                for n in spec.nodes.values():
                    n.inputs = [
                        new_select.name if i == node.name else i for i in n.inputs
                    ]
                changed = True
                break
    return spec


def _columns_in_predicate(predicate: str) -> set[str]:
    """Heuristic: extract bare identifiers from a SQL-like predicate string."""
    import re
    tokens = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", predicate)
    keywords = {"and", "or", "not", "in", "is", "null", "true", "false", "like", "between"}
    return {t for t in tokens if t.lower() not in keywords and not t.replace(".", "").isdigit()}


# ──────────────────────────────────────────────────────────────
# Pass 2 – merge consecutive SELECT nodes
# ──────────────────────────────────────────────────────────────

def _merge_consecutive_selects(spec: PipelineSpec) -> PipelineSpec:
    """
    SELECT(SELECT(x, [a,b,c]), [a,b]) → SELECT(x, [a,b])
    """
    changed = True
    while changed:
        changed = False
        for name, node in list(spec.nodes.items()):
            if node.transform_type != TransformType.SELECT:
                continue
            if len(node.inputs) != 1:
                continue
            parent = spec.nodes.get(node.inputs[0])
            if parent is None or parent.transform_type != TransformType.SELECT:
                continue
            if parent.columns is None or node.columns is None:
                continue

            merged_cols = [c for c in parent.columns if c in node.columns]
            node.columns = merged_cols
            node.inputs = parent.inputs[:]
            del spec.nodes[parent.name]
            for n in spec.nodes.values():
                n.inputs = [name if i == parent.name else i for i in n.inputs]
            changed = True
            break
    return spec


# ──────────────────────────────────────────────────────────────
# Pass 3 – remove SELECT nodes that keep all columns
# ──────────────────────────────────────────────────────────────

def _eliminate_identity_selects(spec: PipelineSpec) -> PipelineSpec:
    to_remove = []
    for name, node in spec.nodes.items():
        if node.transform_type != TransformType.SELECT:
            continue
        if len(node.inputs) != 1:
            continue
        parent = spec.nodes.get(node.inputs[0])
        if parent is None or parent.output_schema is None:
            continue
        if node.columns is None:
            continue
        if set(node.columns) == set(parent.output_schema.field_names()):
            to_remove.append((name, node.inputs[0]))

    for removed, replacement in to_remove:
        del spec.nodes[removed]
        for n in spec.nodes.values():
            n.inputs = [replacement if i == removed else i for i in n.inputs]

    return spec
