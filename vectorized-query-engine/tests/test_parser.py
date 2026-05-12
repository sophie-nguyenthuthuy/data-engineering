"""Test SQL → LogicalPlan parsing."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from vqe.parser import parse
from vqe.logical_plan import Scan, Filter, Project, Aggregate, Sort, Limit, Join
from vqe.expressions import ColumnRef, Literal, BinaryExpr, AggExpr


def test_simple_select_star():
    plan = parse("SELECT * FROM t")
    assert isinstance(plan, Scan)
    assert plan.table == "t"


def test_select_columns():
    plan = parse("SELECT id, name FROM t")
    assert isinstance(plan, Project)
    assert isinstance(plan.child, Scan)
    cols = [e.name for e in plan.exprs if isinstance(e, ColumnRef)]
    assert "id" in cols
    assert "name" in cols


def test_where_filter():
    plan = parse("SELECT * FROM t WHERE score > 80")
    assert isinstance(plan, Filter)
    pred = plan.predicate
    assert isinstance(pred, BinaryExpr)
    assert pred.op == ">"


def test_aggregate_count_star():
    plan = parse("SELECT COUNT(*) FROM t")
    assert isinstance(plan, Project)
    # Project wraps Aggregate
    assert isinstance(plan.child, Aggregate)
    agg = plan.child
    assert len(agg.aggregates) == 1
    assert agg.aggregates[0].func == "count_star"


def test_aggregate_sum_group_by():
    plan = parse("SELECT dept, SUM(score) FROM t GROUP BY dept")
    assert isinstance(plan, Project)
    agg = plan.child
    assert isinstance(agg, Aggregate)
    assert len(agg.group_by) == 1
    assert isinstance(agg.group_by[0], ColumnRef)
    assert agg.group_by[0].name == "dept"
    assert agg.aggregates[0].func == "sum"


def test_order_by():
    plan = parse("SELECT id FROM t ORDER BY score DESC")
    assert isinstance(plan, Sort)
    assert plan.ascending == [False]


def test_limit():
    plan = parse("SELECT id FROM t LIMIT 5")
    assert isinstance(plan, Limit)
    assert plan.n == 5


def test_between():
    plan = parse("SELECT * FROM t WHERE score BETWEEN 70 AND 90")
    assert isinstance(plan, Filter)


def test_join():
    plan = parse("SELECT t.id, u.role FROM t JOIN u ON t.id = u.tid")
    assert isinstance(plan, Project)
    child = plan.child
    assert isinstance(child, Join)
