"""Cost-based optimizer: reorders joins and refines estimates."""

from __future__ import annotations

import itertools

from .nodes import (
    Aggregate, Filter, Join, JoinType, Limit, PlanNode,
    Project, Sort, TableScan,
)

# Relative I/O cost per row for each source type
SOURCE_SCAN_COST = {
    "postgres": 1.0,
    "mongodb": 1.5,
    "s3_parquet": 0.8,   # columnar, cheap for projected reads
    "rest_api": 10.0,    # network round-trips are expensive
}

JOIN_HASH_COST_PER_ROW = 2.0
JOIN_NESTED_LOOP_THRESHOLD = 1_000   # use nested-loop only for tiny tables


class CostBasedOptimizer:
    """
    Performs:
      1. Predicate push-down (already done by the planner, validated here)
      2. Projection push-down (trim unneeded columns early)
      3. Join reordering (smallest estimated output first — greedy)
      4. Cost annotation of every node
    """

    def optimize(self, root: PlanNode) -> PlanNode:
        root = self._push_projections(root)
        root = self._reorder_joins(root)
        root = self._annotate_costs(root)
        return root

    # ------------------------------------------------------------------ #
    # Projection push-down                                                 #
    # ------------------------------------------------------------------ #

    def _push_projections(self, node: PlanNode) -> PlanNode:
        """Walk the tree and trim projected_columns on TableScans."""
        match node:
            case Project(child=child, columns=cols, output_names=names):
                needed = self._columns_needed_by(cols)
                child = self._prune_columns(child, needed)
                node.child = self._push_projections(child)
            case Filter(child=child):
                node.child = self._push_projections(child)
            case Join(left=left, right=right):
                node.left = self._push_projections(left)
                node.right = self._push_projections(right)
            case Aggregate(child=child):
                node.child = self._push_projections(child)
            case Sort(child=child):
                node.child = self._push_projections(child)
            case Limit(child=child):
                node.child = self._push_projections(child)
        return node

    def _columns_needed_by(self, expressions: list) -> set[str]:
        import sqlglot.expressions as exp
        needed: set[str] = set()
        for e in expressions:
            for col in e.find_all(exp.Column):
                needed.add(col.name)
        return needed

    def _prune_columns(self, node: PlanNode, needed: set[str]) -> PlanNode:
        if isinstance(node, TableScan) and node.projected_columns and needed:
            node.projected_columns = [c for c in node.projected_columns if c in needed] \
                or node.projected_columns
        return node

    # ------------------------------------------------------------------ #
    # Join reordering — greedy smallest-first                              #
    # ------------------------------------------------------------------ #

    def _reorder_joins(self, node: PlanNode) -> PlanNode:
        """Collect all INNER join leaves and rebuild left-deep tree by est_rows."""
        match node:
            case Join(join_type=JoinType.INNER):
                leaves, conditions = self._flatten_inner_joins(node)
                if len(leaves) >= 2:
                    leaves.sort(key=lambda n: n.estimated_rows)
                    return self._build_left_deep(leaves, conditions)
            case Filter(child=child):
                node.child = self._reorder_joins(child)
            case Project(child=child):
                node.child = self._reorder_joins(child)
            case Aggregate(child=child):
                node.child = self._reorder_joins(child)
            case Sort(child=child):
                node.child = self._reorder_joins(child)
            case Limit(child=child):
                node.child = self._reorder_joins(child)
        return node

    def _flatten_inner_joins(
        self, node: PlanNode
    ) -> tuple[list[PlanNode], list]:
        if not isinstance(node, Join) or node.join_type != JoinType.INNER:
            return [node], []
        left_leaves, left_conds = self._flatten_inner_joins(node.left)
        right_leaves, right_conds = self._flatten_inner_joins(node.right)
        cond = node.condition
        conds = left_conds + right_conds + ([cond] if cond is not None else [])
        return left_leaves + right_leaves, conds

    def _build_left_deep(self, leaves: list[PlanNode], conditions: list) -> PlanNode:
        node = leaves[0]
        for right in leaves[1:]:
            cond = conditions.pop(0) if conditions else None
            est = max(1, int((node.estimated_rows * right.estimated_rows) ** 0.5))
            # Re-derive join keys from the condition so the executor can hash-join
            left_keys, right_keys = _extract_join_keys_from_condition(cond)
            node = Join(
                left=node,
                right=right,
                join_type=JoinType.INNER,
                condition=cond,
                left_keys=left_keys,
                right_keys=right_keys,
                estimated_rows=est,
            )
        return node

    # ------------------------------------------------------------------ #
    # Cost annotation                                                      #
    # ------------------------------------------------------------------ #

    def _annotate_costs(self, node: PlanNode) -> PlanNode:
        match node:
            case TableScan(source=src, estimated_rows=rows):
                rate = SOURCE_SCAN_COST.get(src, 1.0)
                node.estimated_cost = rows * rate

            case Filter(child=child):
                node.child = self._annotate_costs(child)
                node.estimated_cost = child.estimated_cost + node.estimated_rows * 0.1

            case Project(child=child):
                node.child = self._annotate_costs(child)
                node.estimated_cost = child.estimated_cost

            case Join(left=left, right=right, estimated_rows=rows):
                node.left = self._annotate_costs(left)
                node.right = self._annotate_costs(right)
                build_cost = right.estimated_rows * JOIN_HASH_COST_PER_ROW
                probe_cost = left.estimated_rows * 1.0
                node.estimated_cost = left.estimated_cost + right.estimated_cost + build_cost + probe_cost

            case Aggregate(child=child):
                node.child = self._annotate_costs(child)
                node.estimated_cost = child.estimated_cost + child.estimated_rows * 0.5

            case Sort(child=child):
                node.child = self._annotate_costs(child)
                n = max(1, child.estimated_rows)
                node.estimated_cost = child.estimated_cost + n * (n.bit_length())

            case Limit(child=child):
                node.child = self._annotate_costs(child)
                node.estimated_cost = child.estimated_cost

        return node


# ──────────────────────────────────────────────────────────────────────────────
# Module-level helper
# ──────────────────────────────────────────────────────────────────────────────

def _extract_join_keys_from_condition(condition) -> tuple[list[str], list[str]]:
    """Re-derive left_keys / right_keys from a join ON expression."""
    if condition is None:
        return [], []
    import sqlglot.expressions as exp

    left_keys: list[str] = []
    right_keys: list[str] = []

    for eq in condition.find_all(exp.EQ):
        lhs, rhs = eq.this, eq.expression
        if not (isinstance(lhs, exp.Column) and isinstance(rhs, exp.Column)):
            continue
        lk = f"{lhs.table}.{lhs.name}" if lhs.table else lhs.name
        rk = f"{rhs.table}.{rhs.name}" if rhs.table else rhs.name
        left_keys.append(lk)
        right_keys.append(rk)

    return left_keys, right_keys
