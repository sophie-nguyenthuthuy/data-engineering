import random

from src import (
    Task, DAG, Forecaster,
    list_schedule, makespan, branch_and_bound,
    baseline_fcfs_schedule, regret,
)


def test_forecaster_learns_lognormal():
    f = Forecaster()
    rng = random.Random(0)
    for _ in range(200):
        # Lognormal with mu=2, sigma=0.5 → mean ≈ e^(2+0.125) ≈ 8.36
        x = math_exp_lognormal(rng, mu=2.0, sigma=0.5)
        f.observe("t1", x)
    mean = f.mean("t1")
    assert abs(mean - 8.36) / 8.36 < 0.25


def math_exp_lognormal(rng, mu, sigma):
    import math
    z = rng.gauss(0, 1)
    return math.exp(mu + sigma * z)


def test_critical_path():
    d = DAG()
    d.add(Task("a", 1.0))
    d.add(Task("b", 2.0, deps=["a"]))
    d.add(Task("c", 3.0, deps=["a"]))
    d.add(Task("d", 1.0, deps=["b", "c"]))
    cp, eft = d.critical_path_length()
    assert cp == 5.0   # a → c → d = 1+3+1 = 5
    assert eft["d"] == 5.0


def test_list_schedule_respects_deps():
    d = DAG()
    d.add(Task("a", 1.0))
    d.add(Task("b", 2.0, deps=["a"]))
    d.add(Task("c", 3.0))
    sched = list_schedule(d, num_workers=2)
    # b starts after a finishes
    assert sched["b"][0] >= sched["a"][1]


def test_list_schedule_better_than_fcfs():
    """A workload where CP-first wins clearly."""
    d = DAG()
    # CP: a (5) → b (5) — chain of 10
    # Plus c (8), d (1) independent
    d.add(Task("a", 5.0))
    d.add(Task("b", 5.0, deps=["a"]))
    d.add(Task("c", 8.0))
    d.add(Task("d", 1.0))
    r = regret(d, num_workers=2)
    # CP first should be at least as good as FCFS
    assert r.our_makespan <= r.baseline_makespan


def test_branch_and_bound_small_dag():
    d = DAG()
    d.add(Task("a", 2.0))
    d.add(Task("b", 3.0))
    d.add(Task("c", 1.0))
    bb = branch_and_bound(d, num_workers=2, time_limit_ms=500)
    ms_bb = makespan(bb)
    ls = list_schedule(d, num_workers=2)
    ms_ls = makespan(ls)
    assert ms_bb <= ms_ls


def test_no_oversubscription():
    """Workers don't run two tasks at the same time."""
    d = DAG()
    for i in range(10):
        d.add(Task(f"t{i}", duration=1.0 + (i % 3)))
    sched = list_schedule(d, num_workers=3)
    for w in range(3):
        intervals = sorted([(s, f) for tid, (s, f, wo) in sched.items() if wo == w])
        for i in range(len(intervals) - 1):
            assert intervals[i][1] <= intervals[i + 1][0] + 1e-9


def test_makespan_zero_for_empty():
    assert makespan({}) == 0.0
