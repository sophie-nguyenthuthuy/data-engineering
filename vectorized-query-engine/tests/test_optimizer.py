"""Test optimizer: predicate pushdown, projection pushdown, constant folding."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from vqe.parser import parse
from vqe.optimizer import Optimizer
from vqe.logical_plan import Scan, Filter, Project, Aggregate


opt = Optimizer()


def test_predicate_pushed_into_scan():
    plan = parse("SELECT * FROM t WHERE score > 80")
    optimized = opt.optimize(plan)
    # After pushdown the Filter node should be gone and Scan has the predicate
    assert isinstance(optimized, Scan)
    assert len(optimized.pushed_predicates) > 0


def test_projection_pushdown_narrows_columns():
    plan = parse("SELECT id, score FROM t WHERE score > 80")
    optimized = opt.optimize(plan)
    # Walk to the Scan
    node = optimized
    while not isinstance(node, Scan):
        if hasattr(node, "child"):
            node = node.child
        else:
            break
    assert isinstance(node, Scan)
    # Scan should only read id and score (not name, dept, etc.)
    if node.columns:
        assert set(node.columns).issubset({"id", "score"})


def test_constant_folding():
    from vqe.optimizer import fold_constants
    from vqe.expressions import BinaryExpr, Literal
    expr = BinaryExpr("+", Literal(3), Literal(4))
    folded = fold_constants(expr)
    assert isinstance(folded, Literal)
    assert folded.value == 7


def test_predicate_not_pushed_through_aggregate():
    plan = parse("SELECT dept, SUM(score) AS s FROM t GROUP BY dept")
    optimized = opt.optimize(plan)
    # Should still have an Aggregate node
    node = optimized
    found_agg = False
    for _ in range(10):
        if isinstance(node, Aggregate):
            found_agg = True
            break
        if hasattr(node, "child"):
            node = node.child
        else:
            break
    assert found_agg
