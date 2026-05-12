"""Tests for plan parsing and cardinality error detection."""
import json
from pathlib import Path
import pytest

from cle.plan.parser import (
    parse_explain_json,
    extract_cardinality_errors,
    has_critical_error,
    get_worst_node,
)
from cle.plan.node import PlanNode


FIXTURE = Path(__file__).parent / "fixtures" / "sample_plan.json"


@pytest.fixture
def sample_plan() -> PlanNode:
    data = json.loads(FIXTURE.read_text())
    return parse_explain_json(data)


def test_parse_tree_shape(sample_plan):
    assert sample_plan.node_type == "Hash Join"
    assert len(sample_plan.children) == 2
    assert sample_plan.children[0].node_type == "Seq Scan"
    assert sample_plan.children[1].node_type == "Hash"
    assert sample_plan.children[1].children[0].node_type == "Index Scan"


def test_parse_actuals(sample_plan):
    assert sample_plan.actual_rows == 42
    assert sample_plan.actual_rows_total == 42


def test_q_error_root(sample_plan):
    # root: est=5000, act=42 → q_error = 5000/42 ≈ 119
    qe = sample_plan.q_error
    assert qe is not None
    assert qe > 100


def test_q_error_leaf(sample_plan):
    # Seq Scan: est=50000, act=45000 → q_error ≈ 1.11
    seq = sample_plan.children[0]
    assert seq.q_error is not None
    assert seq.q_error < 2.0


def test_all_nodes(sample_plan):
    nodes = sample_plan.all_nodes()
    assert len(nodes) == 4
    types = {n.node_type for n in nodes}
    assert "Hash Join" in types
    assert "Index Scan" in types


def test_has_critical_error(sample_plan):
    # root has q_error > 100
    assert has_critical_error(sample_plan, threshold=100.0)


def test_no_critical_error_high_threshold(sample_plan):
    assert not has_critical_error(sample_plan, threshold=200.0)


def test_get_worst_node(sample_plan):
    node, qe = get_worst_node(sample_plan)
    assert node.node_type == "Hash Join"
    assert qe > 100


def test_operator_one_hot(sample_plan):
    vec = sample_plan.operator_one_hot()
    assert len(vec) > 0
    assert sum(vec) == 1


def test_node_ids_unique(sample_plan):
    nodes = sample_plan.all_nodes()
    ids = [n.node_id for n in nodes]
    assert len(ids) == len(set(ids))
