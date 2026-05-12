"""
Cascades optimizer – top-down cost-based query optimization with memoization.

Algorithm overview (Graefe 1995)
---------------------------------
OptimizeGroup(G):
    if G.winner is set: return G.winner          # memoized
    for each logical expr E in G:
        ApplyTransformations(E, G)               # expand equivalence class
    for each logical expr E in G:
        ApplyImplementations(E, G)               # generate physical plans
    for each physical expr P in G:
        cost = P.local_cost + sum(OptimizeGroup(child).cost)
        G.update_winner(P, cost)
    return G.winner

Join ordering via the Cascades framework is exponential in theory but
controlled by memoization (each unique sub-plan is costed once) and
pruning (cost bound propagation).  For N=10 we enumerate all bushy trees
via DP over subsets, which is O(3^N) but finishes quickly in Python for N≤12.
"""
from __future__ import annotations
import itertools
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

from optimizer.expressions import (
    LogicalJoin, PhysicalJoin, PhysicalScan, PhysicalOp, Scan, Predicate,
)
from optimizer.memo import Memo, Group, Winner, GroupStats
from optimizer.cost_model import CostModel, CostEstimate
from optimizer.histogram import StatsCatalog


class CascadesOptimizer:
    """
    Subset-DP variant of the Cascades optimizer.

    We enumerate all 2^N subsets of the N relations, bottom-up, and for each
    split of a subset S into (S1, S2) we generate all three physical join
    algorithms, keeping the cheapest winner per subset.

    This is equivalent to Cascades with all transformation rules applied to
    exhaustion (commutativity + associativity), which generates all bushy plans.
    """

    def __init__(self, catalog: StatsCatalog, cost_model: CostModel) -> None:
        self.catalog = catalog
        self.cost_model = cost_model
        self.memo = Memo()
        # subset_id (frozenset of tables) → Group
        self._subset_to_group: Dict[FrozenSet[str], Group] = {}
        self._calls: int = 0  # optimization calls for telemetry

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def optimize(
        self,
        tables: List[str],
        predicates: List[Predicate],
    ) -> Winner:
        """
        Find the optimal join order for `tables` connected by `predicates`.
        Returns the root Winner containing the best physical plan tree.
        """
        self._all_predicates = predicates
        n = len(tables)

        # ---- Phase 1: seed base-table groups ----
        for t in tables:
            g = self.memo.get_or_create_scan(t)
            rows = self.catalog.base_rows(t)
            ts = self.catalog.get(t)
            g.stats = GroupStats(
                row_count=rows,
                avg_row_bytes=ts.avg_row_bytes if ts else 100,
            )
            # Implement the scan immediately
            p = PhysicalScan(t)
            g.add_physical(p)
            cost = self.cost_model.seq_scan(t, rows)
            g.update_winner(p, cost, {})
            self._subset_to_group[frozenset([t])] = g

        # ---- Phase 2: DP over subsets of increasing size ----
        table_list = list(tables)
        for size in range(2, n + 1):
            for subset in itertools.combinations(table_list, size):
                s = frozenset(subset)
                self._optimize_subset(s)

        root_tables = frozenset(tables)
        root_group = self._subset_to_group.get(root_tables)
        if root_group is None or root_group.winner is None:
            raise RuntimeError("Optimizer failed to find any valid plan")
        return root_group.winner

    # ------------------------------------------------------------------
    # Internal DP
    # ------------------------------------------------------------------

    def _optimize_subset(self, tables: FrozenSet[str]) -> None:
        """Enumerate all binary splits of `tables`, cost each join, keep best."""
        best_winner: Optional[Winner] = None
        best_group: Optional[Group] = None

        table_list = sorted(tables)

        # Enumerate all non-empty proper subsets as left side
        for size in range(1, len(tables)):
            for left_combo in itertools.combinations(table_list, size):
                left = frozenset(left_combo)
                right = tables - left

                left_group = self._subset_to_group.get(left)
                right_group = self._subset_to_group.get(right)
                if left_group is None or right_group is None:
                    continue
                if left_group.winner is None or right_group.winner is None:
                    continue

                preds = self._applicable_predicates(left, right)
                out_rows = self.catalog.join_output_rows(
                    left, right,
                    left_group.stats.row_count,
                    right_group.stats.row_count,
                    preds,
                )
                out_bytes = self._estimate_row_bytes(left | right)

                # Create or retrieve the group for this subset
                g = self.memo.get_or_create(tables)
                g.tables = tables
                g.stats = GroupStats(row_count=out_rows, avg_row_bytes=out_bytes)
                self._subset_to_group[tables] = g

                preds_tuple = tuple(preds)
                child_winners = {
                    left_group.id: left_group.winner,
                    right_group.id: right_group.winner,
                }

                # Try all three join algorithms
                for algo, cost_fn in [
                    (PhysicalOp.HASH_JOIN, self._cost_hash),
                    (PhysicalOp.MERGE_JOIN, self._cost_merge),
                    (PhysicalOp.NESTED_LOOP, self._cost_nl),
                ]:
                    if algo == PhysicalOp.MERGE_JOIN and not preds:
                        continue  # merge join needs equi-predicate

                    local_cost = cost_fn(
                        left_group, right_group, out_rows
                    )
                    total_cost = (
                        local_cost
                        + left_group.winner.cost
                        + right_group.winner.cost
                    )

                    phys = PhysicalJoin(
                        left_group.id, right_group.id, algo, preds_tuple
                    )
                    # Only add physical if it's a candidate for best
                    if g.winner is None or total_cost.total < g.winner.cost.total:
                        g.add_physical(phys)
                        g.update_winner(phys, total_cost, child_winners)

                self._calls += 1

    def _applicable_predicates(
        self, left: FrozenSet[str], right: FrozenSet[str]
    ) -> List[Predicate]:
        return [
            p for p in self._all_predicates
            if (p.left_table in left and p.right_table in right)
            or (p.left_table in right and p.right_table in left)
        ]

    def _estimate_row_bytes(self, tables: FrozenSet[str]) -> int:
        total = 0
        for t in tables:
            ts = self.catalog.get(t)
            total += ts.avg_row_bytes if ts else 100
        return max(100, total)

    # ------------------------------------------------------------------
    # Cost helpers  (delegate to CostModel)
    # ------------------------------------------------------------------

    def _cost_hash(self, left_g: Group, right_g: Group, out_rows: float) -> CostEstimate:
        # Put smaller side as build (classic heuristic)
        if left_g.stats.row_count <= right_g.stats.row_count:
            build, probe = left_g, right_g
        else:
            build, probe = right_g, left_g
        return self.cost_model.hash_join(
            build.stats.row_count, probe.stats.row_count, out_rows
        )

    def _cost_merge(self, left_g: Group, right_g: Group, out_rows: float) -> CostEstimate:
        return self.cost_model.merge_join(
            left_g.stats.row_count, right_g.stats.row_count, out_rows
        )

    def _cost_nl(self, left_g: Group, right_g: Group, out_rows: float) -> CostEstimate:
        # Put smaller side as outer for nested loop
        if left_g.stats.row_count <= right_g.stats.row_count:
            outer, inner = left_g, right_g
        else:
            outer, inner = right_g, left_g
        return self.cost_model.nested_loop_join(
            outer.stats.row_count, inner.stats.row_count, out_rows
        )

    # ------------------------------------------------------------------
    # Plan extraction
    # ------------------------------------------------------------------

    def extract_plan(self, winner: Winner, depth: int = 0) -> List[str]:
        """Recursively render the winning plan as an indented string list."""
        indent = "  " * depth
        expr = winner.expr
        lines = [f"{indent}{expr}  [cost={winner.cost}]"]
        for child_gid, child_winner in winner.child_winners.items():
            lines.extend(self.extract_plan(child_winner, depth + 1))
        return lines

    def join_order(self, winner: Winner) -> List[str]:
        """Extract the left-deep join order as a flat list of table names."""
        order: List[str] = []
        self._collect_tables(winner, order)
        return order

    def _collect_tables(self, winner: Winner, result: List[str]) -> None:
        expr = winner.expr
        if isinstance(expr, PhysicalScan):
            result.append(expr.table)
            return
        # Visit children in order: left then right
        if isinstance(expr, PhysicalJoin):
            for cid in [expr.left_group, expr.right_group]:
                if cid in winner.child_winners:
                    self._collect_tables(winner.child_winners[cid], result)
