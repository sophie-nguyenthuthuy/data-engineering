"""Transformation and implementation rules.

Two kinds:
  - TransformationRule: rewrites a logical pattern → another logical pattern
    (e.g. predicate pushdown, join associativity). Operates within Memo.
  - ImplementationRule: rewrites a logical pattern → a physical alternative
    on a specific engine. Adds physical expressions to the group.

Each rule has a `match` method (predicate over a GroupExpression) and an
`apply` method that returns new expressions.

The classic Cascades guarantees: rules are checked once per (group, rule).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from ppc.ir.logical import (
    LogicalAggregate,
    LogicalFilter,
    LogicalJoin,
    LogicalNode,
    LogicalScan,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ppc.cascades.memo import GroupExpression, Memo
    from ppc.engines.base import EngineOp


class Rule(ABC):
    """Base class. `name` is used for one-time fire tracking."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def match(self, gexpr: GroupExpression) -> bool: ...


class TransformationRule(Rule):
    @abstractmethod
    def apply(self, gexpr: GroupExpression, memo: Memo) -> Iterable[tuple[LogicalNode, tuple[int, ...]]]:
        """Return zero-or-more (op, child_group_ids) for new logical alternatives."""


class ImplementationRule(Rule):
    @abstractmethod
    def apply(self, gexpr: GroupExpression, memo: Memo) -> Iterable[tuple[EngineOp, tuple[int, ...]]]:
        """Return zero-or-more (physical_op, child_group_ids)."""


# ---------------------------------------------------------------------------
# Transformation rules
# ---------------------------------------------------------------------------


class PredicatePushdownThroughJoin(TransformationRule):
    """Filter(Join(L, R), p) → Join(Filter(L, p_l), Filter(R, p_r)) when
    `p` references only one side."""

    @property
    def name(self) -> str:
        return "PredicatePushdownThroughJoin"

    def match(self, gexpr: GroupExpression) -> bool:
        if not isinstance(gexpr.op, LogicalFilter):
            return False
        # We need the single child's representative to be a Join
        return True  # actual check requires memo lookup; do it in apply

    def apply(self, gexpr: GroupExpression, memo: Memo):
        f = gexpr.op
        assert isinstance(f, LogicalFilter)
        child_group = memo.group(gexpr.children[0])
        for child_expr in list(child_group.logical_exprs):
            if not isinstance(child_expr.op, LogicalJoin):
                continue
            join = child_expr.op
            l_gid, r_gid = child_expr.children
            l_cols = set(memo.group(l_gid).schema.names)
            r_cols = set(memo.group(r_gid).schema.names)
            refs = f.predicate.referenced_columns()
            if refs.issubset(l_cols):
                # Push into left
                new_left_filter = LogicalFilter(child=join.left, predicate=f.predicate)
                # Need a group for the new Filter(L)
                lf_gid = memo.insert_logical(new_left_filter, (l_gid,))
                new_join = LogicalJoin(left=join.left, right=join.right, on=join.on,
                                       join_type=join.join_type)
                yield (new_join, (lf_gid, r_gid))
            elif refs.issubset(r_cols):
                new_right_filter = LogicalFilter(child=join.right, predicate=f.predicate)
                rf_gid = memo.insert_logical(new_right_filter, (r_gid,))
                new_join = LogicalJoin(left=join.left, right=join.right, on=join.on,
                                       join_type=join.join_type)
                yield (new_join, (l_gid, rf_gid))


class JoinCommutativity(TransformationRule):
    """Join(A, B, on=p) ↔ Join(B, A, on=p)."""

    @property
    def name(self) -> str:
        return "JoinCommutativity"

    def match(self, gexpr: GroupExpression) -> bool:
        return isinstance(gexpr.op, LogicalJoin)

    def apply(self, gexpr: GroupExpression, memo: Memo):
        j = gexpr.op
        assert isinstance(j, LogicalJoin)
        l_gid, r_gid = gexpr.children
        swapped = LogicalJoin(left=j.right, right=j.left, on=j.on, join_type=j.join_type)
        yield (swapped, (r_gid, l_gid))


# ---------------------------------------------------------------------------
# Implementation rules — one per (logical-op, engine) pair
# ---------------------------------------------------------------------------


class ScanImpl(ImplementationRule):
    """LogicalScan -> PhysicalScan on a specific engine."""

    def __init__(self, engine: str):
        self.engine = engine

    @property
    def name(self) -> str:
        return f"ScanImpl[{self.engine}]"

    def match(self, gexpr: GroupExpression) -> bool:
        return isinstance(gexpr.op, LogicalScan)

    def apply(self, gexpr: GroupExpression, memo: Memo):
        from ppc.engines.physical_ops import PhysicalScan

        op = gexpr.op
        assert isinstance(op, LogicalScan)
        yield (PhysicalScan(engine=self.engine, table=op.table, schema=op.table_schema), ())


class FilterImpl(ImplementationRule):
    def __init__(self, engine: str):
        self.engine = engine

    @property
    def name(self) -> str:
        return f"FilterImpl[{self.engine}]"

    def match(self, gexpr: GroupExpression) -> bool:
        return isinstance(gexpr.op, LogicalFilter)

    def apply(self, gexpr: GroupExpression, memo: Memo):
        from ppc.engines.physical_ops import PhysicalFilter

        op = gexpr.op
        assert isinstance(op, LogicalFilter)
        child_schema = memo.group(gexpr.children[0]).schema
        yield (
            PhysicalFilter(
                engine=self.engine,
                predicate=op.predicate,
                schema=child_schema,
            ),
            gexpr.children,
        )


class AggregateImpl(ImplementationRule):
    def __init__(self, engine: str):
        self.engine = engine

    @property
    def name(self) -> str:
        return f"AggregateImpl[{self.engine}]"

    def match(self, gexpr: GroupExpression) -> bool:
        return isinstance(gexpr.op, LogicalAggregate)

    def apply(self, gexpr: GroupExpression, memo: Memo):
        from ppc.engines.physical_ops import PhysicalAggregate

        op = gexpr.op
        assert isinstance(op, LogicalAggregate)
        child_schema = memo.group(gexpr.children[0]).schema
        yield (
            PhysicalAggregate(
                engine=self.engine,
                group_by=op.group_by,
                aggregates=op.aggregates,
                child_schema=child_schema,
                output_schema=op.schema,
            ),
            gexpr.children,
        )


class HashJoinImpl(ImplementationRule):
    def __init__(self, engine: str):
        self.engine = engine

    @property
    def name(self) -> str:
        return f"HashJoinImpl[{self.engine}]"

    def match(self, gexpr: GroupExpression) -> bool:
        return isinstance(gexpr.op, LogicalJoin)

    def apply(self, gexpr: GroupExpression, memo: Memo):
        from ppc.engines.physical_ops import PhysicalHashJoin

        op = gexpr.op
        assert isinstance(op, LogicalJoin)
        l_schema = memo.group(gexpr.children[0]).schema
        r_schema = memo.group(gexpr.children[1]).schema
        yield (
            PhysicalHashJoin(
                engine=self.engine,
                on=op.on,
                join_type=op.join_type,
                left_schema=l_schema,
                right_schema=r_schema,
                output_schema=op.schema,
            ),
            gexpr.children,
        )


# ---------------------------------------------------------------------------
# Default rule set
# ---------------------------------------------------------------------------


def default_transformation_rules() -> list[TransformationRule]:
    return [PredicatePushdownThroughJoin(), JoinCommutativity()]


def default_implementation_rules(engines: list[str]) -> list[ImplementationRule]:
    rules: list[ImplementationRule] = []
    for e in engines:
        rules.extend([
            ScanImpl(e),
            FilterImpl(e),
            AggregateImpl(e),
            HashJoinImpl(e),
        ])
    return rules
