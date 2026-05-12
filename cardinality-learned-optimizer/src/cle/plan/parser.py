"""Parse PostgreSQL EXPLAIN (ANALYZE, FORMAT JSON) output into PlanNode trees."""
from __future__ import annotations
import json
from typing import Any
from .node import PlanNode

_FIELD_MAP = {
    "Node Type": "node_type",
    "Plan Rows": "estimated_rows",
    "Plan Width": "estimated_width",
    "Startup Cost": "estimated_cost_startup",
    "Total Cost": "estimated_cost_total",
    "Relation Name": "relation_name",
    "Alias": "alias",
    "Index Name": "index_name",
    "Join Type": "join_type",
    "Hash Cond": "hash_cond",
    "Merge Cond": "merge_cond",
    "Join Filter": "join_filter",
    "Filter": "filter",
    "Output": "output",
}


def _parse_node(raw: dict[str, Any], depth: int, counter: list[int]) -> PlanNode:
    node = PlanNode(
        node_type=raw.get("Node Type", "Unknown"),
        estimated_rows=float(raw.get("Plan Rows", 1)),
        estimated_width=int(raw.get("Plan Width", 0)),
        estimated_cost_startup=float(raw.get("Startup Cost", 0.0)),
        estimated_cost_total=float(raw.get("Total Cost", 0.0)),
        relation_name=raw.get("Relation Name"),
        alias=raw.get("Alias"),
        index_name=raw.get("Index Name"),
        join_type=raw.get("Join Type"),
        hash_cond=raw.get("Hash Cond"),
        merge_cond=raw.get("Merge Cond"),
        join_filter=raw.get("Join Filter"),
        filter=raw.get("Filter"),
        output=raw.get("Output", []),
        depth=depth,
        node_id=counter[0],
    )
    counter[0] += 1

    # Actual rows from ANALYZE
    if "Actual Rows" in raw:
        node.actual_rows = float(raw["Actual Rows"])
        node.actual_loops = int(raw.get("Actual Loops", 1))

    for child_raw in raw.get("Plans", []):
        child = _parse_node(child_raw, depth + 1, counter)
        node.children.append(child)

    return node


def parse_explain_json(explain_output: str | list | dict) -> PlanNode:
    """Parse the result of EXPLAIN (FORMAT JSON) — accepts raw string or already-parsed object."""
    if isinstance(explain_output, str):
        data = json.loads(explain_output)
    else:
        data = explain_output

    # psycopg2 returns a list of dicts; each dict has a "Plan" key
    if isinstance(data, list):
        data = data[0]
    if "Plan" in data:
        data = data["Plan"]

    return _parse_node(data, depth=0, counter=[0])


def parse_explain_result(rows: list) -> PlanNode:
    """Parse rows returned by psycopg2 from EXPLAIN (ANALYZE, FORMAT JSON)."""
    # rows is [(json_string,), ...] or [({'Plan': ...},), ...]
    first = rows[0][0]
    if isinstance(first, str):
        data = json.loads(first)
    else:
        data = first
    return parse_explain_json(data)


def extract_cardinality_errors(root: PlanNode) -> list[tuple[PlanNode, float]]:
    """Return (node, q_error) for all nodes with actual rows populated."""
    errors = []
    for node in root.all_nodes():
        qe = node.q_error
        if qe is not None:
            errors.append((node, qe))
    return errors


def has_critical_error(root: PlanNode, threshold: float = 100.0) -> bool:
    """True if any node has q-error exceeding threshold (100× rule)."""
    for node, qe in extract_cardinality_errors(root):
        if qe >= threshold:
            return True
    return False


def get_worst_node(root: PlanNode) -> tuple[PlanNode, float] | None:
    errors = extract_cardinality_errors(root)
    if not errors:
        return None
    return max(errors, key=lambda x: x[1])
