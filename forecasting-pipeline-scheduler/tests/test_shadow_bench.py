"""Shadow regret + random-DAG generator tests."""

from __future__ import annotations

import pytest

from fps.bench import random_layered_dag
from fps.dag import DAG, Task
from fps.shadow import RegretReport, regret, regret_over_dags


def test_regret_report_from_makespans():
    r = RegretReport.from_makespans(baseline=10.0, ours=8.0)
    assert r.regret == pytest.approx(2.0)
    assert r.speedup == pytest.approx(10.0 / 8.0)


def test_regret_report_zero_makespan_speedup_is_one():
    r = RegretReport.from_makespans(baseline=0.0, ours=0.0)
    assert r.speedup == 1.0


def test_regret_single_dag_returns_non_negative_for_cp_workload():
    d = DAG()
    d.add(Task("a", 5.0))
    d.add(Task("b", 5.0, deps=("a",)))
    d.add(Task("c", 8.0))
    d.add(Task("d", 1.0))
    r = regret(d, num_workers=2)
    assert r.regret >= 0


def test_regret_over_dags_empty_returns_neutral():
    agg = regret_over_dags([])
    assert agg.n_dags == 0
    assert agg.mean_regret == 0.0
    assert agg.mean_speedup == 1.0
    assert agg.positive_fraction() == 0.0


def test_regret_over_dags_reports_aggregate_speedup():
    dags = [random_layered_dag(n_layers=4, avg_layer_width=3, seed=i) for i in range(30)]
    agg = regret_over_dags(dags, num_workers=2)
    assert agg.n_dags == 30
    # CP-first list scheduling should never lose to FCFS on average.
    assert agg.mean_regret >= -1e-9
    assert agg.mean_speedup >= 0.99


def test_random_layered_dag_validates_layers():
    with pytest.raises(ValueError):
        random_layered_dag(n_layers=0)


def test_random_layered_dag_validates_width():
    with pytest.raises(ValueError):
        random_layered_dag(avg_layer_width=0)


def test_random_layered_dag_validates_max_parents():
    with pytest.raises(ValueError):
        random_layered_dag(max_parents=-1)


def test_random_layered_dag_passes_dag_validity():
    """Generated DAGs must topo-sort without cycles or unknown deps."""
    for seed in range(10):
        d = random_layered_dag(n_layers=5, avg_layer_width=4, seed=seed)
        order = d.topo_order()
        assert len(order) == len(d)


def test_random_layered_dag_has_expected_first_layer_unconstrained():
    d = random_layered_dag(n_layers=3, avg_layer_width=2, seed=0)
    layer0 = [tid for tid in d.tasks if tid.startswith("L0-")]
    for tid in layer0:
        assert d.tasks[tid].deps == ()
