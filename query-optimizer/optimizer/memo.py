"""
Memo table – the core data structure of the Cascades optimizer.

Architecture
------------
  Memo
  └── Group (equivalence class of logical sub-plans)
      ├── logical_exprs  : set[LogicalExpr | PhysicalExpr]  (all equivalent plans)
      ├── physical_exprs : set[PhysicalExpr]
      ├── stats          : GroupStats  (row count, width)
      └── best_plan      : Winner | None  (cheapest physical plan found so far)

Groups are identified by integer IDs.  Child pointers in expressions refer to
group IDs, not directly to expressions, enabling shared sub-plans.

Deduplication
-------------
Two logical expressions are in the same group if they produce the same set of
output rows (same relational algebra result).  We detect duplicates by a
canonical *signature* based on the frozenset of base table names they cover.
This is an approximation sufficient for join-ordering; a full implementation
would use pattern-based equivalence proofs.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

from optimizer.expressions import LogicalExpr, PhysicalExpr, Scan
from optimizer.cost_model import CostEstimate


@dataclass
class GroupStats:
    row_count: float = 1.0
    avg_row_bytes: int = 100

    @property
    def pages(self) -> float:
        from optimizer.cost_model import PAGE_SIZE_BYTES
        return max(1.0, self.row_count * self.avg_row_bytes / PAGE_SIZE_BYTES)


@dataclass
class Winner:
    """Best physical plan for a group under a given required property."""
    expr: PhysicalExpr
    cost: CostEstimate
    # maps each child group_id → its Winner
    child_winners: Dict[int, "Winner"] = field(default_factory=dict)


class Group:
    def __init__(self, group_id: int) -> None:
        self.id: int = group_id
        self.logical_exprs: List[LogicalExpr] = []
        self.physical_exprs: List[PhysicalExpr] = []
        self.stats: GroupStats = GroupStats()
        self.winner: Optional[Winner] = None
        # set of base table names reachable from this group
        self.tables: FrozenSet[str] = frozenset()
        # whether this group has been fully explored
        self.explored: bool = False

    def add_logical(self, expr: LogicalExpr) -> None:
        self.logical_exprs.append(expr)

    def add_physical(self, expr: PhysicalExpr) -> None:
        self.physical_exprs.append(expr)

    def update_winner(self, expr: PhysicalExpr, cost: CostEstimate,
                      child_winners: Dict[int, Winner]) -> bool:
        """Returns True if this is a new best plan."""
        if self.winner is None or cost.total < self.winner.cost.total:
            self.winner = Winner(expr, cost, child_winners)
            return True
        return False

    def __repr__(self) -> str:
        return (f"Group({self.id}, tables={set(self.tables)}, "
                f"rows={self.stats.row_count:.0f}, "
                f"winner={self.winner.expr if self.winner else None})")


class Memo:
    """Central memo table for the Cascades optimizer."""

    def __init__(self) -> None:
        self._groups: Dict[int, Group] = {}
        self._next_id: int = 0
        # signature → group_id for deduplication
        self._sig_to_group: Dict[FrozenSet[str], int] = {}

    # ------------------------------------------------------------------
    # Group management
    # ------------------------------------------------------------------

    def new_group(self, tables: FrozenSet[str]) -> Group:
        """Create a fresh group and register it under its table signature."""
        if tables in self._sig_to_group:
            return self._groups[self._sig_to_group[tables]]
        gid = self._next_id
        self._next_id += 1
        g = Group(gid)
        g.tables = tables
        self._groups[gid] = g
        self._sig_to_group[tables] = gid
        return g

    def get_or_create(self, tables: FrozenSet[str]) -> Group:
        return self.new_group(tables)  # new_group already deduplicates

    def get_group(self, gid: int) -> Group:
        return self._groups[gid]

    def all_groups(self) -> List[Group]:
        return list(self._groups.values())

    def num_groups(self) -> int:
        return len(self._groups)

    # ------------------------------------------------------------------
    # Seed from a logical scan (leaf node)
    # ------------------------------------------------------------------

    def get_or_create_scan(self, table: str) -> Group:
        tables = frozenset([table])
        g = self.get_or_create(tables)
        if not g.logical_exprs:
            g.add_logical(Scan(table))
        return g

    # ------------------------------------------------------------------
    # Debug helpers
    # ------------------------------------------------------------------

    def dump(self) -> str:
        lines = [f"Memo ({self.num_groups()} groups):"]
        for g in sorted(self._groups.values(), key=lambda x: x.id):
            lines.append(f"  {g}")
            for e in g.logical_exprs:
                lines.append(f"    L: {e}")
            for e in g.physical_exprs:
                lines.append(f"    P: {e}")
        return "\n".join(lines)
