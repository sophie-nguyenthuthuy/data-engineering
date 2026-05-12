"""Federated query optimizer: picks the cheapest predicate pushdown configuration."""
from __future__ import annotations

import itertools
from typing import Any, Dict, List, Optional

from dqp.catalog import Catalog
from dqp.cost.model import CostModel, PlanCost
from dqp.cost.statistics import TableStats
from dqp.engines.base import EngineBase, PushdownResult
from dqp.logical_plan import (
    FilterNode,
    JoinNode,
    PlanNode,
    PushedScanNode,
    ScanNode,
    plan_to_str,
)
from dqp.predicate import AndPredicate, Predicate, conjuncts


class FederatedOptimizer:
    """Picks the cheapest predicate pushdown plan for each table scan.

    Strategy:
    1. Try pushing all predicates into the engine (full pushdown).
    2. Try pushing no predicates (full table scan + residual filter).
    3. For AND lists, try all subsets of size k (k = 1..n-1).
    4. Return the PushedScanNode with the lowest total cost.
    """

    def __init__(
        self,
        catalog: Catalog,
        cost_model: CostModel,
        engines: Dict[str, EngineBase],
    ) -> None:
        self._catalog = catalog
        self._cost_model = cost_model
        self._engines = engines  # engine_name → EngineBase

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def optimize(
        self,
        table_name: str,
        predicates: List[Predicate],
        columns: List[str],
    ) -> PushedScanNode:
        """Return the lowest-cost PushedScanNode for a single table scan."""
        schema = self._catalog.get_table(table_name)
        engine = self._engines.get(schema.engine_name)
        if engine is None:
            raise KeyError(
                f"Engine {schema.engine_name!r} not registered; "
                f"available: {list(self._engines.keys())}"
            )

        table_stats = self._cost_model._get_stats(table_name)

        # Flatten AND trees so we can try subsets of conjuncts
        flat_preds: List[Predicate] = []
        for p in predicates:
            flat_preds.extend(conjuncts(p))

        best_node: Optional[PushedScanNode] = None
        best_cost: Optional[float] = None

        # Generate candidate split configurations:
        #   Each configuration is a frozenset of indices into flat_preds
        #   that represent the "pushed" subset (rest are residual).
        n = len(flat_preds)
        candidate_pushed_sets: List[List[int]] = []

        # Full pushdown and no pushdown
        candidate_pushed_sets.append(list(range(n)))  # push all
        candidate_pushed_sets.append([])              # push none

        # Partial subsets (for small n, enumerate all; for large n, greedy)
        if 0 < n <= 10:
            for size in range(1, n):
                for combo in itertools.combinations(range(n), size):
                    candidate_pushed_sets.append(list(combo))
        elif n > 10:
            # Greedy: push each predicate individually to rank by benefit
            for i in range(n):
                candidate_pushed_sets.append([i])

        # Deduplicate
        seen = set()
        unique_sets: List[List[int]] = []
        for s in candidate_pushed_sets:
            key = frozenset(s)
            if key not in seen:
                seen.add(key)
                unique_sets.append(s)

        for pushed_indices in unique_sets:
            pushed = [flat_preds[i] for i in pushed_indices]
            residual = [flat_preds[i] for i in range(n) if i not in set(pushed_indices)]

            # Filter pushed to only what the engine can actually push
            actually_pushed = [p for p in pushed if engine.can_push(p)]
            actually_residual = residual + [p for p in pushed if not engine.can_push(p)]

            cost = self._estimate_plan_cost(
                table_name, schema.engine_name, actually_pushed, actually_residual, table_stats
            )
            if best_cost is None or cost.total() < best_cost:
                best_cost = cost.total()
                best_node = PushedScanNode(
                    table_name=table_name,
                    engine_name=schema.engine_name,
                    pushed_predicates=actually_pushed,
                    residual_predicates=actually_residual,
                    columns=columns,
                )

        assert best_node is not None
        return best_node

    def optimize_join(
        self,
        left_table: str,
        right_table: str,
        join_pred: Predicate,
        filter_preds: List[Predicate],
        columns: List[str],
    ) -> PlanNode:
        """Basic join optimization: push per-table filters, then join.

        *filter_preds* are split by which table they reference; predicates
        referencing both tables remain as post-join filters.
        """
        from dqp.predicate import columns_referenced

        left_schema = self._catalog.get_table(left_table)
        right_schema = self._catalog.get_table(right_table)

        left_cols_in_schema = set(left_schema.column_names())
        right_cols_in_schema = set(right_schema.column_names())

        left_preds: List[Predicate] = []
        right_preds: List[Predicate] = []
        cross_preds: List[Predicate] = []

        for pred in filter_preds:
            col_refs = columns_referenced(pred)
            # First try to route by explicit table qualifier on the ColumnRef
            table_qualifiers = {r.table for r in col_refs if r.table is not None}
            if table_qualifiers == {left_table}:
                left_preds.append(pred)
                continue
            if table_qualifiers == {right_table}:
                right_preds.append(pred)
                continue
            # Fall back to column-name membership when qualifiers are absent/ambiguous
            refs = {r.column for r in col_refs}
            in_left = bool(refs & left_cols_in_schema)
            in_right = bool(refs & right_cols_in_schema)
            if in_left and not in_right:
                left_preds.append(pred)
            elif in_right and not in_left:
                right_preds.append(pred)
            else:
                cross_preds.append(pred)

        # Determine which columns each side needs
        left_col_set = {c for c in columns if c in left_cols_in_schema}
        right_col_set = {c for c in columns if c in right_cols_in_schema}
        # Also include join key columns
        for ref in columns_referenced(join_pred):
            if ref.column in left_cols_in_schema:
                left_col_set.add(ref.column)
            if ref.column in right_cols_in_schema:
                right_col_set.add(ref.column)

        left_node = self.optimize(left_table, left_preds, sorted(left_col_set))
        right_node = self.optimize(right_table, right_preds, sorted(right_col_set))

        join_node: PlanNode = JoinNode(
            left=left_node,
            right=right_node,
            condition=join_pred,
            join_type="inner",
        )

        # Apply any cross-table filter predicates above the join
        if cross_preds:
            combined = AndPredicate(cross_preds) if len(cross_preds) > 1 else cross_preds[0]
            join_node = FilterNode(child=join_node, predicate=combined)

        return join_node

    def explain(self, plan_node: PlanNode) -> str:
        """Return a human-readable explanation of the plan with cost estimates."""
        lines = ["=== Query Plan ===", plan_to_str(plan_node), "", "=== Cost Breakdown ==="]
        self._explain_node(plan_node, lines, depth=0)
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _estimate_plan_cost(
        self,
        table_name: str,
        engine_name: str,
        pushed: List[Predicate],
        residual: List[Predicate],
        table_stats: Optional[TableStats],
    ) -> PlanCost:
        if table_stats is None:
            # No stats: use raw scan cost, penalise residual predicates
            scan = self._cost_model.cost_scan(table_name, engine_name)
            # Assume 50% selectivity per residual predicate
            penalty = 1.0
            for _ in residual:
                penalty *= 0.5
            rows_out = scan.rows_out * (0.5 ** max(len(pushed), 1)) * penalty
            return PlanCost(
                cpu_cost=scan.cpu_cost,
                io_cost=scan.io_cost * (0.5 ** len(pushed)),
                rows_out=rows_out,
            )
        return self._cost_model.cost_pushed_scan(
            table_name, engine_name, pushed, residual, table_stats
        )

    def _explain_node(self, node: PlanNode, lines: List[str], depth: int) -> None:
        indent = "  " * depth

        if isinstance(node, PushedScanNode):
            lines.append(f"{indent}PushedScan: {node.table_name!r} via {node.engine_name!r}")
            lines.append(f"{indent}  Pushed predicates ({len(node.pushed_predicates)}):")
            for p in node.pushed_predicates:
                lines.append(f"{indent}    {p!r}")
            lines.append(f"{indent}  Residual predicates ({len(node.residual_predicates)}):")
            for p in node.residual_predicates:
                lines.append(f"{indent}    {p!r}")

            ts = self._cost_model._get_stats(node.table_name)
            if ts:
                cost = self._cost_model.cost_pushed_scan(
                    node.table_name,
                    node.engine_name,
                    node.pushed_predicates,
                    node.residual_predicates,
                    ts,
                )
                lines.append(f"{indent}  Estimated cost: {cost}")

        elif isinstance(node, ScanNode):
            lines.append(f"{indent}Scan: {node.table_name!r} via {node.engine_name!r}")
            cost = self._cost_model.cost_scan(node.table_name, node.engine_name)
            lines.append(f"{indent}  Estimated cost: {cost}")

        elif isinstance(node, FilterNode):
            lines.append(f"{indent}Filter: {node.predicate!r}")
            self._explain_node(node.child, lines, depth + 1)

        elif isinstance(node, JoinNode):
            lines.append(f"{indent}Join ({node.join_type}): {node.condition!r}")
            self._explain_node(node.left, lines, depth + 1)
            self._explain_node(node.right, lines, depth + 1)

        else:
            lines.append(f"{indent}{node!r}")
            for child in node.children:
                self._explain_node(child, lines, depth + 1)
