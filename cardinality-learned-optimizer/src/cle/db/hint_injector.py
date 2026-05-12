"""Build pg_hint_plan hint strings from model cardinality predictions.

pg_hint_plan hint syntax reference:
  Rows(t1 t2 #100)    — set exact row estimate for join of t1,t2
  Rows(t1 *10)        — multiply existing estimate by 10
  SeqScan(t1)         — force seq scan on t1
  IndexScan(t1)       — force index scan on t1
  HashJoin(t1 t2)     — force hash join
  NestLoop(t1 t2)     — force nested loop
  MergeJoin(t1 t2)    — force merge join
  Leading(t1 t2 t3)   — force join order
"""
from __future__ import annotations
import math
from typing import Optional

from ..plan.node import PlanNode


def rows_hint(tables: list[str], rows: float) -> str:
    """Exact row count hint for a set of tables."""
    return f"Rows({' '.join(tables)} #{int(max(rows, 1))})"


def build_cardinality_hints(
    root: PlanNode,
    predicted_rows: dict[int, float],
    threshold: float = 10.0,
) -> str:
    """Generate Rows() hints for nodes whose estimate differs from prediction by > threshold.

    predicted_rows: {node_id: predicted_actual_rows}
    """
    hints: list[str] = []
    for node in root.all_nodes():
        pred = predicted_rows.get(node.node_id)
        if pred is None:
            continue
        ratio = max(node.estimated_rows, 1) / max(pred, 1)
        if ratio > threshold or ratio < 1 / threshold:
            tables = _collect_tables(node)
            if tables:
                hints.append(rows_hint(tables, pred))
    return " ".join(hints)


def build_correction_hints(root: PlanNode, threshold: float = 100.0) -> str:
    """After EXPLAIN ANALYZE, build hints to fix nodes with q-error >= threshold."""
    hints: list[str] = []
    for node in root.all_nodes():
        if node.actual_rows_total is None:
            continue
        if node.q_error and node.q_error >= threshold:
            tables = _collect_tables(node)
            if tables:
                hints.append(rows_hint(tables, node.actual_rows_total))
    return " ".join(hints)


def _collect_tables(node: PlanNode) -> list[str]:
    """Collect all leaf relation names under this node (for Rows() hint)."""
    tables = []
    for n in node.all_nodes():
        if n.relation_name:
            t = n.alias or n.relation_name
            if t not in tables:
                tables.append(t)
    return tables


# ── Bao-style hint sets ───────────────────────────────────────────────────────
# 15 canonical hint combinations from the Bao paper.
# Each is a frozenset of disabled operators.

BAO_HINT_SETS: list[dict] = [
    # 0: default (no hints)
    {},
    # 1-4: disable one join method
    {"disable": ["HashJoin"]},
    {"disable": ["MergeJoin"]},
    {"disable": ["NestLoop"]},
    {"disable": ["HashJoin", "MergeJoin"]},
    # 5-8: scan method variants
    {"disable": ["SeqScan"]},
    {"disable": ["IndexScan"]},
    {"disable": ["SeqScan", "IndexScan"]},          # force bitmap
    {"disable": ["HashJoin", "SeqScan"]},
    # 9-12: combined
    {"disable": ["MergeJoin", "SeqScan"]},
    {"disable": ["NestLoop", "SeqScan"]},
    {"disable": ["HashJoin", "MergeJoin", "SeqScan"]},
    {"disable": ["NestLoop", "MergeJoin"]},
    {"disable": ["NestLoop", "HashJoin"]},
    # 14: everything disabled except NestLoop + IndexScan (forces indexed NL)
    {"disable": ["HashJoin", "MergeJoin", "SeqScan"]},
]


def hint_set_to_sql(hint_set: dict) -> str:
    """Convert a hint set dict to pg_hint_plan GUC string."""
    parts = []
    for op in hint_set.get("disable", []):
        parts.append(f"enable_{op.lower().replace(' ', '')}=off")
    if not parts:
        return ""
    return " SET " + " SET ".join(parts)


def hint_set_to_pg_hints(hint_set: dict) -> str:
    """Build a pg_hint_plan comment block for the hint set."""
    # pg_hint_plan uses /*+ ... */ syntax but for disabling operators we use GUCs
    # Alternative: use Set() hints in pg_hint_plan
    parts = []
    guc_map = {
        "HashJoin": "enable_hashjoin",
        "MergeJoin": "enable_mergejoin",
        "NestLoop": "enable_nestloop",
        "SeqScan": "enable_seqscan",
        "IndexScan": "enable_indexscan",
        "BitmapScan": "enable_bitmapscan",
    }
    for op in hint_set.get("disable", []):
        guc = guc_map.get(op)
        if guc:
            parts.append(f"Set({guc} off)")
    return " ".join(parts)


def apply_hint_set_to_connection(conn_pool, hint_set: dict) -> None:
    """Apply hint set GUCs to current session."""
    guc_map = {
        "HashJoin": "enable_hashjoin",
        "MergeJoin": "enable_mergejoin",
        "NestLoop": "enable_nestloop",
        "SeqScan": "enable_seqscan",
        "IndexScan": "enable_indexscan",
    }
    for op in hint_set.get("disable", []):
        guc = guc_map.get(op)
        if guc:
            conn_pool.execute(f"SET {guc} = off")


def reset_hint_set(conn_pool) -> None:
    for guc in [
        "enable_hashjoin", "enable_mergejoin", "enable_nestloop",
        "enable_seqscan", "enable_indexscan",
    ]:
        conn_pool.execute(f"RESET {guc}")
