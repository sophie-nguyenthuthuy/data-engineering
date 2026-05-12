"""Top-down memoized search with dominance pruning.

Algorithm:
  optimize_group(g, req_props):
    if best_for(g, req_props) cached: return
    1. Fire all transformation rules on g's logical exprs (once per rule×group)
    2. Fire all implementation rules to produce physical alternatives
    3. For each physical alternative:
       a. Recursively optimize its child groups under derived required props
       b. Insert a conversion op if delivered_engine != required engine
       c. Compute total cost = own + child costs + conversion cost
       d. Update best_for(g, req_props) if cheaper
    Done.

This is the classic Cascades pattern without rule groups / promises /
optimization barrier (we add those incrementally if needed).
"""

from __future__ import annotations

from dataclasses import dataclass

from ppc.cascades.memo import Group, GroupExpression, Memo, _OptResult
from ppc.cascades.properties import PhysicalProperties
from ppc.cascades.rules import (
    ImplementationRule,
    TransformationRule,
    default_implementation_rules,
    default_transformation_rules,
)
from ppc.engines.conversions import ConversionRegistry, default_conversion_registry
from ppc.engines.physical_ops import (
    PhysicalAggregate,
    PhysicalConversion,
    PhysicalFilter,
    PhysicalHashJoin,
    PhysicalScan,
)
from ppc.frontend.catalog import Catalog
from ppc.ir.logical import LogicalNode
from ppc.ir.physical import PhysicalNode, PhysicalPlan


DEFAULT_ENGINES = ("spark", "dbt", "duckdb", "flink")


@dataclass
class Optimizer:
    catalog: Catalog
    engines: tuple[str, ...] = DEFAULT_ENGINES
    transformations: list[TransformationRule] | None = None
    implementations: list[ImplementationRule] | None = None
    conversions: ConversionRegistry | None = None

    def __post_init__(self) -> None:
        if self.transformations is None:
            self.transformations = default_transformation_rules()
        if self.implementations is None:
            self.implementations = default_implementation_rules(list(self.engines))
        if self.conversions is None:
            self.conversions = default_conversion_registry()

    # ---- Entry point ------------------------------------------------------

    def optimize(self, logical: LogicalNode) -> PhysicalPlan:
        memo = Memo()
        root_gid = _ingest(logical, memo)
        # Optimise with "any engine" required at root — we'll let the planner
        # pick the cheapest delivered engine.
        req = PhysicalProperties.any()
        result = self._optimize_group(memo, root_gid, req)
        if result is None:
            raise RuntimeError("optimizer failed to find a plan")
        root_phys = self._materialize(memo, result, req)
        return PhysicalPlan(
            root=root_phys,
            total_cost=result.cost,
            estimated_bytes=memo.group(root_gid).schema.bytes_estimate()
            if memo.group(root_gid).schema is not None
            else float("nan"),
            logical=logical,
        )

    # ---- Recursive search -------------------------------------------------

    def _optimize_group(
        self, memo: Memo, gid: int, req: PhysicalProperties
    ) -> _OptResult | None:
        g = memo.group(gid)
        cached = g.best_for_props.get(req)
        if cached is not None:
            return cached

        # Apply transformations until no new logical alternatives appear
        self._explore_transformations(memo, gid)

        # Try every implementation rule on every logical alternative
        best: _OptResult | None = None
        # Snapshot logical_exprs since we may insert new ones during transforms
        logical_snapshot = list(g.logical_exprs)
        for lexpr in logical_snapshot:
            for rule in self.implementations or ():
                if not rule.match(lexpr):
                    continue
                for phys_op, child_gids in rule.apply(lexpr, memo):
                    pexpr = memo.add_physical(gid, phys_op, child_gids)
                    candidate = self._cost_physical(memo, pexpr, req)
                    if candidate is None:
                        continue
                    if best is None or candidate.cost < best.cost:
                        best = candidate
        if best is not None:
            g.best_for_props[req] = best
        return best

    def _explore_transformations(self, memo: Memo, gid: int) -> None:
        """Apply transformations to every logical expression in the group
        until no new alternatives appear. Each rule fires at most once per
        (group, rule) by tracking a `fired` set on the group."""
        if not hasattr(memo.group(gid), "_fired_rules"):
            memo.group(gid)._fired_rules = set()  # type: ignore[attr-defined]
        fired: set[tuple[int, str]] = memo.group(gid)._fired_rules  # type: ignore[attr-defined]

        # Process logical exprs one at a time; the list grows as new are added.
        i = 0
        while True:
            g = memo.group(gid)
            if i >= len(g.logical_exprs):
                break
            lexpr = g.logical_exprs[i]
            for rule in self.transformations or ():
                key = (id(lexpr), rule.name)
                if key in fired:
                    continue
                fired.add(key)
                if not rule.match(lexpr):
                    continue
                for new_op, new_children in rule.apply(lexpr, memo):
                    memo.add_logical(gid, new_op, new_children)
            i += 1

    # ---- Costing ----------------------------------------------------------

    def _cost_physical(
        self, memo: Memo, pexpr: GroupExpression, req: PhysicalProperties
    ) -> _OptResult | None:
        """Compute cost of a physical expression, recursively optimising
        children and inserting conversions where engines mismatch."""
        from ppc.cost.calibrated import CalibratedCostModel

        op = pexpr.op
        assert isinstance(op, (PhysicalScan, PhysicalFilter, PhysicalAggregate, PhysicalHashJoin))
        engine = op.engine

        # Required props for children: same engine, no specific partitioning
        child_req = PhysicalProperties.on(engine)

        total = 0.0
        children_results: list[_OptResult] = []
        for cgid in pexpr.children:
            cres = self._optimize_group(memo, cgid, child_req)
            if cres is None:
                return None
            # Check if conversion is needed (child delivered ≠ required)
            if cres.delivered.engine != engine:
                conv_cost = (self.conversions or default_conversion_registry()).cost(
                    cres.delivered.engine, engine, cres.expr.op.bytes_out  # type: ignore[union-attr]
                )
                total += conv_cost
            total += cres.cost
            children_results.append(cres)

        # Own cost from calibrated model
        cost_model = CalibratedCostModel()
        own_cost = cost_model.cost_of(op)
        total += own_cost

        # Delivered engine = op's engine. Pruning: skip if doesn't meet req.
        delivered = PhysicalProperties.on(engine)
        if not delivered.satisfies(req):
            # If req.engine != "any" and != engine, we'd need a conversion
            # at the top — handled by caller via _materialize. But we still
            # need to add its cost. For simplicity: only consider plans
            # where the top op's engine matches req when req is engine-pinned.
            if req.engine != "any":
                return None

        return _OptResult(expr=pexpr, cost=total, delivered=delivered)

    # ---- Materialization --------------------------------------------------

    def _materialize(
        self, memo: Memo, result: _OptResult, req: PhysicalProperties
    ) -> PhysicalNode:
        op = result.expr.op
        assert isinstance(op, PhysicalNode)
        child_nodes: list[PhysicalNode] = []
        for cgid in result.expr.children:
            child_req = PhysicalProperties.on(op.engine)
            cres = memo.group(cgid).best_for_props[child_req]
            child = self._materialize(memo, cres, child_req)
            if cres.delivered.engine != op.engine:
                child = PhysicalConversion(
                    src_engine=cres.delivered.engine,
                    dst_engine=op.engine,
                    child=child,
                    bytes_in=cres.expr.op.bytes_out,  # type: ignore[union-attr]
                )
            child_nodes.append(child)

        # Attach children to the op (each Physical*Op has a `with_children` method)
        if hasattr(op, "with_children"):
            return op.with_children(tuple(child_nodes))  # type: ignore[attr-defined]
        return op


# ---------------------------------------------------------------------------
# Ingest: convert a logical-plan tree into the Memo
# ---------------------------------------------------------------------------


def _ingest(node: LogicalNode, memo: Memo) -> int:
    child_gids = tuple(_ingest(c, memo) for c in node.children)
    # Insert a "shallow" copy without children for memo identity (memo tracks
    # children separately via group IDs).
    return memo.insert_logical(node, child_gids)
