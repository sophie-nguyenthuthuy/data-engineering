"""Memo: deduplicated DAG of equivalent expressions.

In Cascades, the memo is a hash map from `Group ID -> Group`. Each group
holds:
  - the canonical *logical* expression for that group (and any equivalent
    logical forms produced by transformation rules)
  - a set of *physical* expressions (produced by implementation rules)
  - the best physical expression discovered so far for any given required
    set of properties

GroupExpressions point to child groups (NOT child expressions) — this is
what makes Cascades memoized: identical sub-plans share a group.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ppc.cascades.properties import PhysicalProperties
    from ppc.ir.logical import LogicalNode
    from ppc.ir.physical import PhysicalNode


@dataclass
class GroupExpression:
    """One realisation of a Group's logical content.

    `op` is the logical-or-physical operator (without children).
    `children` is a tuple of group IDs.
    """

    op: LogicalNode | PhysicalNode
    children: tuple[int, ...]           # group IDs
    group_id: int = -1                  # set after insertion into memo
    is_physical: bool = False

    @property
    def kind(self) -> str:
        return type(self.op).__name__


@dataclass
class _OptResult:
    expr: GroupExpression
    cost: float
    delivered: PhysicalProperties


@dataclass
class Group:
    """An equivalence class of expressions producing the same logical result."""

    group_id: int
    logical_exprs: list[GroupExpression] = field(default_factory=list)
    physical_exprs: list[GroupExpression] = field(default_factory=list)
    # Best plan under each required properties: req -> (expr_id, cost, delivered)
    best_for_props: dict[PhysicalProperties, _OptResult] = field(default_factory=dict)
    # Pinned schema (from the original logical plan) for cost-model use
    schema: Any = None  # actually Schema, but kept Any to avoid import cycle


@dataclass
class Memo:
    groups: list[Group] = field(default_factory=list)
    _by_key: dict[tuple[Any, ...], int] = field(default_factory=dict)

    def insert_logical(self, op: LogicalNode, child_groups: tuple[int, ...]) -> int:
        """Insert a logical expression; returns the group ID it landed in."""
        key = ("logical", type(op).__name__, _op_key(op), child_groups)
        if key in self._by_key:
            return self._by_key[key]
        gid = len(self.groups)
        g = Group(group_id=gid, schema=op.schema)
        expr = GroupExpression(op=op, children=child_groups, group_id=gid, is_physical=False)
        g.logical_exprs.append(expr)
        self.groups.append(g)
        self._by_key[key] = gid
        return gid

    def add_physical(self, group_id: int, op: PhysicalNode, child_groups: tuple[int, ...]) -> GroupExpression:
        """Add a physical alternative to an existing group."""
        expr = GroupExpression(op=op, children=child_groups, group_id=group_id, is_physical=True)
        self.groups[group_id].physical_exprs.append(expr)
        return expr

    def add_logical(self, group_id: int, op: LogicalNode, child_groups: tuple[int, ...]) -> GroupExpression | None:
        """Add a transformed logical alternative; dedupes on (op, children)."""
        key = ("logical", type(op).__name__, _op_key(op), child_groups)
        if key in self._by_key and self._by_key[key] != group_id:
            # This logical is already in a different group — would need merge
            # (we do not merge groups in this simplified Cascades).
            return None
        if key in self._by_key:
            # Already present — no-op
            return None
        expr = GroupExpression(op=op, children=child_groups, group_id=group_id, is_physical=False)
        self.groups[group_id].logical_exprs.append(expr)
        self._by_key[key] = group_id
        return expr

    def group(self, gid: int) -> Group:
        return self.groups[gid]


def _op_key(op: LogicalNode | PhysicalNode) -> Any:
    """Deterministic per-op identity key for dedup.

    Per-op key excludes children (those are tracked via group IDs) and any
    schema fields (those are derived from inputs).
    """
    from ppc.ir.logical import (
        LogicalAggregate,
        LogicalFilter,
        LogicalJoin,
        LogicalScan,
    )

    if isinstance(op, LogicalScan):
        return ("Scan", op.table)
    if isinstance(op, LogicalFilter):
        return ("Filter", op.predicate)
    if isinstance(op, LogicalAggregate):
        return ("Aggregate", op.group_by, op.aggregates)
    if isinstance(op, LogicalJoin):
        return ("Join", op.on, op.join_type)
    return ("Other", repr(op))
