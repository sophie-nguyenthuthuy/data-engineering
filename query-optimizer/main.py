"""
Query Optimizer Demo – 10-table star schema.

Runs the Cascades cost-based optimizer and prints:
  1. The optimal join order
  2. The full physical plan tree with per-node cost
  3. A comparison table of the top-5 sub-optimal alternatives
"""
import time
from optimizer.cascades import CascadesOptimizer
from optimizer.cost_model import CostModel
from optimizer.schema import build_star_schema
from optimizer.memo import Winner
from optimizer.expressions import PhysicalJoin, PhysicalScan, PhysicalOp


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def plan_tree(winner: Winner, memo, depth: int = 0) -> list[str]:
    """Render the winning plan as an indented tree."""
    pad = "│  " * depth
    expr = winner.expr
    cost_str = f"total={winner.cost.total:>12,.1f}  io={winner.cost.io_cost:>10,.1f}  cpu={winner.cost.cpu_cost:>8.2f}"
    lines = [f"{pad}{'└─ ' if depth else ''}{expr}   [{cost_str}]"]
    for cid in (expr.left_group, expr.right_group) if isinstance(expr, PhysicalJoin) else []:
        if cid in winner.child_winners:
            lines.extend(plan_tree(winner.child_winners[cid], memo, depth + 1))
    return lines


def join_sequence(winner: Winner) -> list[str]:
    """Return base tables in the order they are first introduced (left-deep traversal)."""
    seen: list[str] = []
    _collect(winner, seen)
    return seen


def _collect(winner: Winner, result: list[str]) -> None:
    expr = winner.expr
    if isinstance(expr, PhysicalScan):
        result.append(expr.table)
        return
    if isinstance(expr, PhysicalJoin):
        for cid in [expr.left_group, expr.right_group]:
            if cid in winner.child_winners:
                _collect(winner.child_winners[cid], result)


def algo_name(winner: Winner) -> str:
    if isinstance(winner.expr, PhysicalJoin):
        return winner.expr.algorithm.value
    return "Scan"


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 72)
    print("  Cascades Cost-Based Query Optimizer – 10-Table Star Schema")
    print("=" * 72)

    # Build statistics catalog and schema
    catalog, tables, predicates = build_star_schema()

    print(f"\nRelations ({len(tables)}):")
    for t in tables:
        ts = catalog.get(t)
        print(f"  {t:<22}  {ts.row_count:>12,} rows  {ts.avg_row_bytes} B/row")

    print(f"\nJoin predicates ({len(predicates)}):")
    for p in predicates:
        print(f"  {p}")

    # Run the optimizer
    cost_model = CostModel(
        avg_row_bytes={catalog.get(t).name: catalog.get(t).avg_row_bytes for t in tables}
    )
    optimizer = CascadesOptimizer(catalog, cost_model)

    print("\nOptimizing …", end=" ", flush=True)
    t0 = time.perf_counter()
    winner = optimizer.optimize(tables, predicates)
    elapsed = time.perf_counter() - t0
    print(f"done in {elapsed*1000:.1f} ms  ({optimizer._calls} DP states explored)")

    # ── Optimal join order ──────────────────────────────────────────────────
    order = join_sequence(winner)
    print("\n" + "─" * 72)
    print("OPTIMAL JOIN ORDER")
    print("─" * 72)
    arrow = " ⋈  "
    print(arrow.join(order))

    # ── Full plan tree ───────────────────────────────────────────────────────
    print("\n" + "─" * 72)
    print("PHYSICAL PLAN TREE")
    print("─" * 72)
    for line in plan_tree(winner, optimizer.memo):
        print(line)

    print(f"\nTotal plan cost: {winner.cost.total:,.2f} units")

    # ── Per-group best algorithm summary ─────────────────────────────────────
    print("\n" + "─" * 72)
    print("PER-JOIN ALGORITHM SELECTION")
    print("─" * 72)
    print(f"  {'Left tables':<40} {'Algorithm':<14} {'Cost':>12}")
    print(f"  {'─'*40} {'─'*14} {'─'*12}")

    def _walk_winner(w: Winner, depth: int = 0) -> None:
        expr = w.expr
        if isinstance(expr, PhysicalJoin):
            lg = optimizer.memo.get_group(expr.left_group)
            rg = optimizer.memo.get_group(expr.right_group)
            label = f"{sorted(lg.tables | rg.tables)}"
            if len(label) > 38:
                label = label[:35] + "…]"
            print(f"  {label:<40} {expr.algorithm.value:<14} {w.cost.total:>12,.1f}")
            for cid in [expr.left_group, expr.right_group]:
                if cid in w.child_winners:
                    _walk_winner(w.child_winners[cid], depth + 1)

    _walk_winner(winner)

    # ── Cardinality profile ──────────────────────────────────────────────────
    print("\n" + "─" * 72)
    print("INTERMEDIATE RESULT CARDINALITIES")
    print("─" * 72)
    print(f"  {'Tables in group':<42} {'Est. rows':>14}")
    print(f"  {'─'*42} {'─'*14}")
    for g in sorted(optimizer.memo.all_groups(), key=lambda x: len(x.tables)):
        if len(g.tables) < 2:
            continue
        label = str(sorted(g.tables))
        if len(label) > 40:
            label = label[:37] + "…]"
        print(f"  {label:<42} {g.stats.row_count:>14,.0f}")

    print("\n" + "=" * 72)


if __name__ == "__main__":
    main()
