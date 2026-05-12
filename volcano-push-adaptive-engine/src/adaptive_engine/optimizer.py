"""Cost-based optimizer and runtime re-optimizer.

Two responsibilities:

1. **Initial optimization** – annotate a logical plan with estimated row
   counts and assign unique node IDs before execution begins.

2. **Runtime re-optimization** – called when actual cardinality diverges
   from estimates by more than a threshold.  May:
     * Swap join sides (probe ↔ build) when the probe side is smaller
     * Push filters closer to their source
     * Upgrade NestedLoopJoin → HashJoin when the input size justifies it
"""
from __future__ import annotations
import itertools
from typing import Any

from .catalog import Catalog
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
    walk,
)


_id_counter = itertools.count(1)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{next(_id_counter)}"


# ------------------------------------------------------------------
# Initial optimizer
# ------------------------------------------------------------------

class Optimizer:
    """Annotates a plan tree with estimated cardinalities and node IDs."""

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    def optimize(self, root: PlanNode) -> PlanNode:
        self._annotate(root)
        return root

    # ------------------------------------------------------------------
    # Estimation
    # ------------------------------------------------------------------

    def _annotate(self, node: PlanNode) -> int:
        """Return estimated output rows for *node*, setting node.estimated_rows."""
        node.node_id = _new_id(type(node).__name__)

        match node:
            case ScanNode():
                est = self.catalog.stats(node.table).row_count
            case FilterNode():
                assert node.child
                child_est = self._annotate(node.child)
                # Prefer histogram-derived selectivity over the caller-supplied guess
                sel = _filter_selectivity(node, self.catalog)
                if sel is not None:
                    node.selectivity = sel
                est = max(1, int(child_est * node.selectivity))
            case ProjectNode():
                assert node.child
                est = self._annotate(node.child)
            case HashJoinNode():
                assert node.left and node.right
                left_est = self._annotate(node.left)
                right_est = self._annotate(node.right)
                # Simple estimate: min(left, right) * selectivity heuristic
                est = max(1, int(left_est * _join_selectivity(left_est, right_est)))
            case NestedLoopJoinNode():
                assert node.left and node.right
                left_est = self._annotate(node.left)
                right_est = self._annotate(node.right)
                est = max(1, int(left_est * right_est * 0.1))
            case AggregateNode():
                assert node.child
                self._annotate(node.child)
                # Groups are at most child rows; often much fewer
                est = max(1, len(node.group_by) * 10)
            case SortNode() | LimitNode():
                assert node.child
                est = self._annotate(node.child)
                if isinstance(node, LimitNode):
                    est = min(est, node.limit)
            case BufferNode():
                est = len(node.rows)
            case _:
                est = 0

        node.estimated_rows = est
        return est


def _join_selectivity(left: int, right: int) -> float:
    """Very rough join selectivity: assume 10% of probe side matches."""
    return 0.1


def _filter_selectivity(node: "FilterNode", catalog: "Catalog") -> float | None:
    """Derive filter selectivity from catalog histograms when possible.

    Walks the predicate looking for simple (col op literal) comparisons
    anchored to a scan child.  Returns None if it can't improve on the
    caller-supplied estimate.
    """
    from .expressions import BinOp, ColRef, Literal, AndExpr, OrExpr
    from .plan import ScanNode

    # Find the nearest scan in the child chain to know the table name
    table: str | None = None
    child = node.child
    while child is not None:
        if isinstance(child, ScanNode):
            table = child.table
            break
        children = child.children()
        child = children[0] if children else None

    if table is None or node.predicate is None:
        return None

    try:
        tbl_stats = catalog.stats(table)
    except KeyError:
        return None

    return _pred_selectivity(node.predicate, tbl_stats)


def _pred_selectivity(pred: Any, tbl_stats: Any) -> float | None:
    from .expressions import BinOp, ColRef, Literal, AndExpr, OrExpr, NotExpr

    if isinstance(pred, BinOp):
        left, op, right = pred.left, pred.op, pred.right
        # col op literal
        if isinstance(left, ColRef) and isinstance(right, Literal):
            col = tbl_stats.column(left.name)
            if col is not None:
                return col.selectivity_for_op(op, right.value)
        # literal op col (reverse)
        if isinstance(right, ColRef) and isinstance(left, Literal):
            col = tbl_stats.column(right.name)
            rev = {"<": ">", ">": "<", "<=": ">=", ">=": "<=", "=": "=", "!=": "!="}
            if col is not None and op in rev:
                return col.selectivity_for_op(rev[op], left.value)
        return None

    if isinstance(pred, AndExpr):
        sels = [_pred_selectivity(p, tbl_stats) for p in pred.preds]
        known = [s for s in sels if s is not None]
        if not known:
            return None
        result = 1.0
        for s in known:
            result *= s
        return result

    if isinstance(pred, OrExpr):
        sels = [_pred_selectivity(p, tbl_stats) for p in pred.preds]
        known = [s for s in sels if s is not None]
        if not known:
            return None
        result = 0.0
        for s in known:
            result = result + s - result * s  # inclusion-exclusion
        return min(1.0, result)

    if isinstance(pred, NotExpr):
        inner = _pred_selectivity(pred.pred, tbl_stats)
        return (1.0 - inner) if inner is not None else None

    return None


# ------------------------------------------------------------------
# Runtime re-optimizer
# ------------------------------------------------------------------

class ReOptimizer:
    """Rewrites a plan subtree based on observed runtime statistics.

    Strategies applied (in order):
    1. Swap HashJoin sides when probe > build (bad build-side choice).
    2. Push FilterNode closer to its source scan.
    3. Upgrade NestedLoopJoin → HashJoin when inputs are large.
    """

    def __init__(self, catalog: Catalog, hot_threshold: float = 10.0) -> None:
        self.catalog = catalog
        self.hot_threshold = hot_threshold
        self.reoptimizations: list[str] = []

    def reoptimize(
        self,
        root: PlanNode,
        actual_counts: dict[str, int],
    ) -> PlanNode:
        """Return a (possibly rewritten) plan tree.

        actual_counts maps node_id -> actual rows seen so far.
        """
        self.reoptimizations.clear()
        root = self._rewrite(root, actual_counts)
        Optimizer(self.catalog).optimize(root)  # re-annotate estimates
        return root

    # ------------------------------------------------------------------
    # Rewrite passes
    # ------------------------------------------------------------------

    def _rewrite(self, node: PlanNode, counts: dict[str, int]) -> PlanNode:
        # Bottom-up rewrite
        for attr in ("child", "left", "right"):
            child = getattr(node, attr, None)
            if isinstance(child, PlanNode):
                setattr(node, attr, self._rewrite(child, counts))

        node = self._swap_join_sides(node, counts)
        node = self._upgrade_nl_join(node, counts)
        node = self._push_filter_down(node)
        return node

    def _swap_join_sides(self, node: PlanNode, counts: dict[str, int]) -> PlanNode:
        if not isinstance(node, HashJoinNode):
            return node
        assert node.left and node.right

        left_actual = counts.get(node.left.node_id, node.left.estimated_rows)
        right_actual = counts.get(node.right.node_id, node.right.estimated_rows)

        # Build side should be the *smaller* relation
        if left_actual < right_actual * 0.5:
            self.reoptimizations.append(
                f"Swap HashJoin sides: probe={node.left.node_id} "
                f"({left_actual}) ↔ build={node.right.node_id} ({right_actual})"
            )
            node.left, node.right = node.right, node.left
            node.left_key, node.right_key = node.right_key, node.left_key

        return node

    def _upgrade_nl_join(self, node: PlanNode, counts: dict[str, int]) -> PlanNode:
        if not isinstance(node, NestedLoopJoinNode):
            return node
        assert node.left and node.right

        left_actual = counts.get(node.left.node_id, node.left.estimated_rows)
        right_actual = counts.get(node.right.node_id, node.right.estimated_rows)

        # NL join is O(n²) — upgrade when either side exceeds 1 000 rows
        if left_actual > 1_000 or right_actual > 1_000:
            self.reoptimizations.append(
                f"Upgrade NLJoin → HashJoin (left={left_actual}, right={right_actual})"
            )
            # We need an equi-join predicate; skip if none available
            # (In practice we'd extract from the predicate; here we skip the upgrade
            #  if the predicate doesn't look like an equality)
            pred = node.predicate
            if pred and hasattr(pred, "op") and pred.op == "=":
                left_key = pred.left.name if hasattr(pred.left, "name") else ""
                right_key = pred.right.name if hasattr(pred.right, "name") else ""
                if left_key and right_key:
                    return HashJoinNode(
                        left=node.left,
                        right=node.right,
                        left_key=left_key,
                        right_key=right_key,
                        estimated_rows=node.estimated_rows,
                        node_id=node.node_id,
                    )

        return node

    def _push_filter_down(self, node: PlanNode) -> PlanNode:
        """Hoist a FilterNode that sits above another FilterNode: merge them."""
        if not isinstance(node, FilterNode):
            return node
        if isinstance(node.child, FilterNode):
            # Merge into a single AndExpr filter at the lower level
            from .expressions import AndExpr
            inner: FilterNode = node.child  # type: ignore[assignment]
            assert inner.predicate and node.predicate
            inner.predicate = AndExpr(inner.predicate, node.predicate)
            inner.selectivity = inner.selectivity * node.selectivity
            inner.estimated_rows = node.estimated_rows
            self.reoptimizations.append(
                f"Merged stacked filters into {inner.node_id}"
            )
            return inner
        return node
