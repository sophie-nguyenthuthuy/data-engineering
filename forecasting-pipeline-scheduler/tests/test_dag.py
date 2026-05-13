"""DAG primitive tests."""

from __future__ import annotations

import pytest

from fps.dag import DAG, CycleError, Task


def test_task_rejects_empty_id():
    with pytest.raises(ValueError):
        Task("", 1.0)


def test_task_rejects_negative_duration():
    with pytest.raises(ValueError):
        Task("a", -1.0)


def test_task_rejects_self_dependency():
    with pytest.raises(ValueError):
        Task("a", 1.0, deps=("a",))


def test_task_rejects_duplicate_deps():
    with pytest.raises(ValueError):
        Task("a", 1.0, deps=("b", "b"))


def test_dag_rejects_duplicate_task():
    d = DAG()
    d.add(Task("a", 1.0))
    with pytest.raises(ValueError):
        d.add(Task("a", 2.0))


def test_dag_membership():
    d = DAG()
    d.add(Task("a", 1.0))
    assert "a" in d
    assert "b" not in d
    assert len(d) == 1


def test_topo_basic():
    d = DAG()
    d.add(Task("a", 1.0))
    d.add(Task("b", 1.0, deps=("a",)))
    d.add(Task("c", 1.0, deps=("b",)))
    assert d.topo_order() == ["a", "b", "c"]


def test_topo_detects_cycle():
    d = DAG()
    d.add(Task("a", 1.0, deps=("b",)))
    d.add(Task("b", 1.0, deps=("a",)))
    with pytest.raises(CycleError):
        d.topo_order()


def test_topo_detects_unknown_dependency():
    d = DAG()
    d.add(Task("a", 1.0, deps=("nope",)))
    with pytest.raises(CycleError):
        d.topo_order()


def test_topo_is_deterministic():
    d = DAG()
    d.add(Task("c", 1.0))
    d.add(Task("a", 1.0))
    d.add(Task("b", 1.0))
    # Roots are sorted ascending → 'a', 'b', 'c'.
    assert d.topo_order() == ["a", "b", "c"]


def test_critical_path_diamond():
    d = DAG()
    d.add(Task("a", 1.0))
    d.add(Task("b", 2.0, deps=("a",)))
    d.add(Task("c", 3.0, deps=("a",)))
    d.add(Task("d", 1.0, deps=("b", "c")))
    cp, eft = d.critical_path_length()
    assert cp == pytest.approx(5.0)
    assert eft["d"] == pytest.approx(5.0)


def test_critical_path_empty_dag():
    cp, eft = DAG().critical_path_length()
    assert cp == 0.0 and eft == {}


def test_successors_unknown_dep_raises():
    d = DAG()
    d.add(Task("a", 1.0, deps=("ghost",)))
    with pytest.raises(CycleError):
        d.successors()
