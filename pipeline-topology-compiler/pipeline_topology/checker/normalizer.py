"""
Algebraic normalizer for pipeline DAGs.

Reduces a PipelineSpec to a canonical form so that two semantically equivalent
pipelines compare equal even if they differ structurally (e.g. one has had
filter-pushdown applied, the other hasn't).

Normalization steps
-------------------
1. Inline trivial aliases  – a SELECT that renames a single node with the same
   column set is removed and references to it are rewritten.
2. Predicate conjunction ordering  – multi-clause AND predicates are sorted
   alphabetically so "a > 1 AND b < 2" == "b < 2 AND a > 1".
3. Aggregate signature  – aggregations are sorted by output_name so two
   AGGREGATE nodes that compute the same set of aggregations in different
   declaration order still compare equal.
4. Column-set normalization  – SELECT column lists are sorted.
"""
from __future__ import annotations

import copy
import re
from typing import Optional

from ..dsl.ir import PipelineSpec, TransformNode
from ..dsl.types import TransformType


def normalize(spec: PipelineSpec) -> PipelineSpec:
    spec = copy.deepcopy(spec)
    spec = _inline_identity_selects(spec)
    spec = _normalize_predicates(spec)
    spec = _normalize_aggregation_order(spec)
    spec = _normalize_select_columns(spec)
    return spec


def _inline_identity_selects(spec: PipelineSpec) -> PipelineSpec:
    """Remove SELECT nodes whose column list equals their input schema's fields."""
    to_remove: list[tuple[str, str]] = []
    for name, node in spec.nodes.items():
        if node.transform_type != TransformType.SELECT:
            continue
        if not node.columns or len(node.inputs) != 1:
            continue
        parent = spec.nodes.get(node.inputs[0])
        if parent is None or parent.output_schema is None:
            continue
        if set(node.columns) == set(parent.output_schema.field_names()):
            to_remove.append((name, node.inputs[0]))

    for removed, replacement in to_remove:
        del spec.nodes[removed]
        for n in spec.nodes.values():
            n.inputs = [replacement if i == removed else i for i in n.inputs]

    return spec


def _normalize_predicates(spec: PipelineSpec) -> PipelineSpec:
    """Canonicalize predicate strings: normalize whitespace, sort AND clauses."""
    for node in spec.nodes.values():
        if node.predicate:
            node.predicate = _canonical_predicate(node.predicate)
    return spec


def _canonical_predicate(pred: str) -> str:
    pred = re.sub(r"\s+", " ", pred.strip())
    # Split on top-level AND and sort clauses
    clauses = _split_and(pred)
    return " AND ".join(sorted(c.strip() for c in clauses))


def _split_and(pred: str) -> list[str]:
    """Split on AND at depth 0 (not inside parentheses)."""
    depth, start, parts = 0, 0, []
    i = 0
    upper = pred.upper()
    while i < len(pred):
        if pred[i] == "(":
            depth += 1
        elif pred[i] == ")":
            depth -= 1
        elif depth == 0 and upper[i:i+4] == "AND " and (i == 0 or pred[i-1] == " "):
            parts.append(pred[start:i].strip())
            start = i + 4
        i += 1
    parts.append(pred[start:].strip())
    return [p for p in parts if p]


def _normalize_aggregation_order(spec: PipelineSpec) -> PipelineSpec:
    for node in spec.nodes.values():
        if node.aggregations:
            node.aggregations = sorted(node.aggregations, key=lambda a: a.output_name)
    return spec


def _normalize_select_columns(spec: PipelineSpec) -> PipelineSpec:
    for node in spec.nodes.values():
        if node.transform_type == TransformType.SELECT and node.columns:
            node.columns = sorted(node.columns)
    return spec
