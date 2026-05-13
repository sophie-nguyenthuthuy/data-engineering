"""Closed-loop agent + benchmark tests."""

from __future__ import annotations

import numpy as np
import pytest

from llo.agent.loop import LayoutAgent
from llo.bench import evaluate_static, make_dataset, make_shifted_workload
from llo.policy.bandit import Action, HeuristicPolicy, UCBPolicy
from llo.workload.profile import Query, WorkloadProfile


def _setup_agent(policy_cls: str = "ucb"):
    data, cols = make_dataset(n_rows=512, seed=0)
    profile = WorkloadProfile(columns=cols)
    if policy_cls == "ucb":
        actions = [
            Action("noop", ()),
            Action("zorder", ("a", "b")),
            Action("sortkey", ("a",)),
        ]
        policy = UCBPolicy(actions=actions)
    else:
        policy = HeuristicPolicy(profile=profile)
    agent = LayoutAgent(data=data, columns=cols, policy=policy, profile=profile)
    return agent, cols


def test_agent_validates_data_shape():
    data = np.zeros((4,), dtype=np.int64)
    p = WorkloadProfile(columns=["a"])
    with pytest.raises(ValueError):
        LayoutAgent(
            data=data,  # type: ignore[arg-type]
            columns=["a"],
            policy=HeuristicPolicy(profile=p),
            profile=p,
        )


def test_agent_validates_column_count():
    data = np.zeros((4, 3), dtype=np.int64)
    p = WorkloadProfile(columns=["a", "b"])
    with pytest.raises(ValueError):
        LayoutAgent(data=data, columns=["a", "b"], policy=HeuristicPolicy(profile=p), profile=p)


def test_agent_validates_window():
    data = np.zeros((4, 1), dtype=np.int64)
    p = WorkloadProfile(columns=["a"])
    with pytest.raises(ValueError):
        LayoutAgent(
            data=data,
            columns=["a"],
            policy=HeuristicPolicy(profile=p),
            profile=p,
            window=0,
        )


def test_agent_observe_updates_profile_and_recent():
    agent, _ = _setup_agent("ucb")
    q = Query({"a": ("=", 1.0)})
    agent.observe(q)
    assert agent.profile.n == 1
    assert len(agent._recent) == 1


def test_agent_recent_is_capped_at_window():
    data, cols = make_dataset(n_rows=128, seed=0)
    p = WorkloadProfile(columns=cols)
    agent = LayoutAgent(
        data=data,
        columns=cols,
        policy=HeuristicPolicy(profile=p),
        profile=p,
        window=10,
    )
    for _ in range(50):
        agent.observe(Query({"a": ("=", 1.0)}))
    assert len(agent._recent) == 10


def test_agent_step_logs_and_increments():
    agent, _ = _setup_agent("ucb")
    for _ in range(20):
        agent.observe(Query({"a": ("range", 10.0, 30.0), "b": ("range", 10.0, 30.0)}))
    log = agent.step()
    assert log.step == 0
    assert log.action.kind in {"noop", "zorder", "sortkey"}
    assert len(agent.history) == 1


def test_agent_run_streams_and_acts():
    agent, _ = _setup_agent("ucb")
    wl = [Query({"a": ("=", 1.0)}) for _ in range(150)]
    history = agent.run(wl, act_every=50)
    assert len(history) == 3
    assert all(h.action.kind in {"noop", "zorder", "sortkey"} for h in history)


def test_agent_run_rejects_zero_period():
    agent, _ = _setup_agent("ucb")
    with pytest.raises(ValueError):
        agent.run([], act_every=0)


def test_agent_drift_event_triggers_after_shift():
    data, cols = make_dataset(n_rows=128, seed=0)
    p = WorkloadProfile(columns=cols)
    actions = [Action("noop", ()), Action("sortkey", ("a",)), Action("sortkey", ("c",))]
    agent = LayoutAgent(
        data=data,
        columns=cols,
        policy=UCBPolicy(actions=actions),
        profile=p,
    )
    # Phase 1: hammer column 'a'.
    for _ in range(80):
        agent.observe(Query({"a": ("=", 1.0)}))
    agent.step()  # baseline
    # Phase 2: switch to column 'c' — large drift.
    for _ in range(80):
        agent.observe(Query({"c": ("=", 1.0)}))
    agent.step()
    assert any(log.drift_event for log in agent.history)


def test_agent_avg_reward_improves_over_baseline():
    """UCB should — on average — beat the noop baseline on a stable
    workload that admits a useful layout."""
    data, cols = make_dataset(n_rows=512, seed=0)
    p = WorkloadProfile(columns=cols)
    actions = [
        Action("noop", ()),
        Action("zorder", ("a", "b")),
        Action("hilbert", ("a", "b")),
    ]
    agent = LayoutAgent(data=data, columns=cols, policy=UCBPolicy(actions=actions), profile=p)
    wl = [Query({"a": ("range", 50.0, 80.0), "b": ("range", 50.0, 80.0)}) for _ in range(400)]
    agent.run(wl, act_every=20)
    nontrivial = [h.reward for h in agent.history if h.action.kind != "noop"]
    assert nontrivial, "UCB should have explored at least one non-noop action"
    assert sum(nontrivial) / len(nontrivial) > 0


def test_make_shifted_workload_phases():
    cols = ["a", "b", "c", "d"]
    wl = make_shifted_workload(cols, n_queries=400, shift_every=100)
    # Last query in phase 0 must reference (a, b); first in phase 1 must reference (b, c).
    phase0_cols = set(wl[0].predicates.keys())
    phase1_cols = set(wl[100].predicates.keys())
    assert phase0_cols != phase1_cols


def test_evaluate_static_zorder_beats_noop_on_box_workload():
    data, cols = make_dataset(n_rows=2048, seed=3)
    wl = make_shifted_workload(cols, n_queries=200, shift_every=200)  # no shift
    r_noop = evaluate_static(data, cols, Action("noop", ()), wl, "noop")
    r_z = evaluate_static(data, cols, Action("zorder", ("a", "b")), wl, "z")
    assert r_z.mean_pages <= r_noop.mean_pages
