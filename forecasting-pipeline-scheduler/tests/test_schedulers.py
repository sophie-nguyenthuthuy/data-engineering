"""Scheduler tests + Hypothesis property invariants."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from fps.bench import random_layered_dag
from fps.dag import DAG, Task
from fps.scheduler.baseline import baseline_fcfs_schedule
from fps.scheduler.branch_bound import branch_and_bound
from fps.scheduler.common import (
    ScheduledTask,
    ScheduleInvariantError,
    assert_valid_schedule,
    makespan,
)
from fps.scheduler.list_sched import list_schedule


def _chain(durations: list[float]) -> DAG:
    d = DAG()
    prev = None
    for i, dur in enumerate(durations):
        tid = f"t{i}"
        d.add(Task(id=tid, duration=dur, deps=(prev,) if prev else ()))
        prev = tid
    return d


# ----------------------------------------------------------- common types


def test_scheduled_task_rejects_negative_start():
    with pytest.raises(ValueError):
        ScheduledTask(start=-1.0, finish=0.0, worker=0)


def test_scheduled_task_rejects_finish_before_start():
    with pytest.raises(ValueError):
        ScheduledTask(start=2.0, finish=1.0, worker=0)


def test_scheduled_task_rejects_negative_worker():
    with pytest.raises(ValueError):
        ScheduledTask(start=0.0, finish=1.0, worker=-1)


def test_makespan_empty_is_zero():
    assert makespan({}) == 0.0


# ------------------------------------------------------- list scheduler


def test_list_schedule_rejects_zero_workers():
    with pytest.raises(ValueError):
        list_schedule(DAG(), num_workers=0)


def test_list_schedule_empty_dag():
    assert list_schedule(DAG(), num_workers=2) == {}


def test_list_schedule_respects_deps():
    d = DAG()
    d.add(Task("a", 1.0))
    d.add(Task("b", 2.0, deps=("a",)))
    d.add(Task("c", 3.0))
    s = list_schedule(d, num_workers=2)
    assert s["b"].start >= s["a"].finish - 1e-12


def test_list_schedule_meets_cp_lower_bound():
    d = _chain([2.0, 3.0, 4.0])
    s = list_schedule(d, num_workers=4)
    cp = sum(t.duration for t in d.tasks.values())  # chain → CP = sum
    assert makespan(s) == pytest.approx(cp)


def test_list_schedule_beats_or_matches_fcfs_on_critical_path_workload():
    d = DAG()
    d.add(Task("a", 5.0))
    d.add(Task("b", 5.0, deps=("a",)))
    d.add(Task("c", 8.0))
    d.add(Task("d", 1.0))
    s_ls = list_schedule(d, num_workers=2)
    s_base = baseline_fcfs_schedule(d, num_workers=2)
    assert makespan(s_ls) <= makespan(s_base)


def test_list_schedule_passes_validity_invariants():
    d = random_layered_dag(n_layers=4, avg_layer_width=3, seed=0)
    s = list_schedule(d, num_workers=3)
    assert_valid_schedule(d, s, num_workers=3)


# --------------------------------------------------------- baseline


def test_baseline_rejects_zero_workers():
    with pytest.raises(ValueError):
        baseline_fcfs_schedule(DAG(), num_workers=0)


def test_baseline_empty_dag():
    assert baseline_fcfs_schedule(DAG(), num_workers=2) == {}


def test_baseline_passes_validity_invariants():
    d = random_layered_dag(n_layers=4, avg_layer_width=3, seed=1)
    s = baseline_fcfs_schedule(d, num_workers=3)
    assert_valid_schedule(d, s, num_workers=3)


# --------------------------------------------------------- B&B


def test_branch_and_bound_rejects_zero_workers():
    with pytest.raises(ValueError):
        branch_and_bound(DAG(), num_workers=0)


def test_branch_and_bound_rejects_zero_time_limit():
    with pytest.raises(ValueError):
        branch_and_bound(DAG(), num_workers=1, time_limit_ms=0)


def test_branch_and_bound_empty_dag():
    assert branch_and_bound(DAG(), num_workers=2) == {}


def test_branch_and_bound_at_most_list_schedule_makespan_small_dag():
    d = DAG()
    d.add(Task("a", 2.0))
    d.add(Task("b", 3.0))
    d.add(Task("c", 1.0))
    bb = branch_and_bound(d, num_workers=2, time_limit_ms=200)
    ls = list_schedule(d, num_workers=2)
    assert makespan(bb) <= makespan(ls) + 1e-9


def test_branch_and_bound_falls_back_on_large_dag():
    """When the DAG exceeds max_tasks, B&B returns the list_schedule result."""
    d = DAG()
    for i in range(20):
        d.add(Task(id=f"t{i}", duration=1.0))
    bb = branch_and_bound(d, num_workers=2, time_limit_ms=50, max_tasks=12)
    ls = list_schedule(d, num_workers=2)
    assert makespan(bb) == pytest.approx(makespan(ls))


def test_branch_and_bound_passes_validity_invariants():
    d = DAG()
    d.add(Task("a", 2.0))
    d.add(Task("b", 3.0, deps=("a",)))
    d.add(Task("c", 1.0, deps=("a",)))
    d.add(Task("d", 4.0, deps=("b", "c")))
    s = branch_and_bound(d, num_workers=2, time_limit_ms=200)
    assert_valid_schedule(d, s, num_workers=2)


# ----------------------------------------- invariant + Hypothesis property


def test_assert_valid_rejects_missing_task():
    d = DAG()
    d.add(Task("a", 1.0))
    with pytest.raises(ScheduleInvariantError):
        assert_valid_schedule(d, {}, num_workers=1)


def test_assert_valid_rejects_dep_violation():
    d = DAG()
    d.add(Task("a", 1.0))
    d.add(Task("b", 1.0, deps=("a",)))
    bad = {
        "a": ScheduledTask(start=5.0, finish=6.0, worker=0),
        "b": ScheduledTask(start=0.0, finish=1.0, worker=1),
    }
    with pytest.raises(ScheduleInvariantError):
        assert_valid_schedule(d, bad, num_workers=2)


def test_assert_valid_rejects_worker_overlap():
    d = DAG()
    d.add(Task("a", 1.0))
    d.add(Task("b", 1.0))
    bad = {
        "a": ScheduledTask(start=0.0, finish=2.0, worker=0),
        "b": ScheduledTask(start=1.0, finish=2.0, worker=0),
    }
    with pytest.raises(ScheduleInvariantError):
        assert_valid_schedule(d, bad, num_workers=1)


@settings(max_examples=25, deadline=None)
@given(
    n_layers=st.integers(min_value=1, max_value=5),
    avg_width=st.integers(min_value=1, max_value=5),
    workers=st.integers(min_value=1, max_value=4),
    seed=st.integers(min_value=0, max_value=2**16 - 1),
)
def test_property_list_schedule_is_always_valid(n_layers, avg_width, workers, seed):
    d = random_layered_dag(n_layers=n_layers, avg_layer_width=avg_width, seed=seed)
    s = list_schedule(d, num_workers=workers)
    assert_valid_schedule(d, s, num_workers=workers)


@settings(max_examples=20, deadline=None)
@given(
    n_layers=st.integers(min_value=1, max_value=5),
    avg_width=st.integers(min_value=1, max_value=5),
    workers=st.integers(min_value=1, max_value=4),
    seed=st.integers(min_value=0, max_value=2**16 - 1),
)
def test_property_makespan_ge_critical_path(n_layers, avg_width, workers, seed):
    d = random_layered_dag(n_layers=n_layers, avg_layer_width=avg_width, seed=seed)
    cp, _ = d.critical_path_length()
    s = list_schedule(d, num_workers=workers)
    assert makespan(s) >= cp - 1e-9
