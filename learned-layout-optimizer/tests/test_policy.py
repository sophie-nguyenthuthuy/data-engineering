"""Policy + bandit tests."""

from __future__ import annotations

import random

import pytest

from llo.policy.bandit import (
    Action,
    EpsilonGreedyPolicy,
    HeuristicPolicy,
    ThompsonPolicy,
    UCBPolicy,
    heuristic_action,
)
from llo.workload.profile import Query, WorkloadProfile

# -------------------------------------------------------- Action invariants


def test_action_noop_rejects_columns():
    with pytest.raises(ValueError):
        Action("noop", ("a",))


def test_action_sortkey_requires_one_column():
    with pytest.raises(ValueError):
        Action("sortkey", ())
    with pytest.raises(ValueError):
        Action("sortkey", ("a", "b"))


def test_action_zorder_requires_two_columns():
    with pytest.raises(ValueError):
        Action("zorder", ("a",))


def test_action_hilbert_requires_two_columns():
    with pytest.raises(ValueError):
        Action("hilbert", ("a",))


def test_action_repr_is_stable():
    a = Action("zorder", ("x", "y"))
    assert repr(a) == "zorder(x,y)"
    assert repr(Action("noop", ())) == "noop"


# ---------------------------------------------------------- heuristic


def test_heuristic_returns_noop_on_empty_profile():
    p = WorkloadProfile(columns=["a", "b"])
    assert heuristic_action(p).kind == "noop"


def test_heuristic_picks_sortkey_with_single_hot_col():
    p = WorkloadProfile(columns=["a", "b"])
    for _ in range(5):
        p.observe(Query({"a": ("=", 1.0)}))
    a = heuristic_action(p)
    assert a.kind == "sortkey" and a.cols == ("a",)


def test_heuristic_picks_hilbert_when_both_range():
    p = WorkloadProfile(columns=["a", "b"])
    for _ in range(10):
        p.observe(Query({"a": ("range", 0.0, 1.0), "b": ("range", 0.0, 1.0)}))
    a = heuristic_action(p)
    assert a.kind == "hilbert"
    assert set(a.cols) == {"a", "b"}


def test_heuristic_picks_zorder_when_mixed():
    p = WorkloadProfile(columns=["a", "b"])
    for _ in range(10):
        p.observe(Query({"a": ("=", 1.0), "b": ("range", 0.0, 1.0)}))
    a = heuristic_action(p)
    assert a.kind == "zorder"


def test_heuristic_policy_wrapper_returns_action():
    p = WorkloadProfile(columns=["a"])
    pol = HeuristicPolicy(profile=p)
    assert pol.choose().kind == "noop"
    pol.update(pol.choose(), 0.5)  # no-op


# --------------------------------------------------------------- UCB1


def test_ucb_rejects_empty_actions():
    with pytest.raises(ValueError):
        UCBPolicy(actions=[])


def test_ucb_rejects_non_positive_c():
    with pytest.raises(ValueError):
        UCBPolicy(actions=[Action("noop", ())], c=0)


def test_ucb_plays_every_action_first():
    actions = [Action("noop", ()), Action("sortkey", ("a",))]
    pol = UCBPolicy(actions=actions)
    first = pol.choose()
    pol.update(first, 0.0)
    second = pol.choose()
    pol.update(second, 0.0)
    assert {repr(first), repr(second)} == {repr(a) for a in actions}


def test_ucb_converges_to_best_arm():
    actions = [
        Action("noop", ()),
        Action("zorder", ("x", "y")),
        Action("sortkey", ("x",)),
    ]
    pol = UCBPolicy(actions=actions, c=0.5)
    rewards = {
        repr(actions[0]): 0.1,
        repr(actions[1]): 1.0,
        repr(actions[2]): 0.5,
    }
    for _ in range(400):
        a = pol.choose()
        pol.update(a, rewards[repr(a)])
    plays = pol._plays
    assert plays[repr(actions[1])] > plays[repr(actions[0])]
    assert plays[repr(actions[1])] > plays[repr(actions[2])]


# ---------------------------------------------------------- ε-greedy


def test_epsilon_greedy_rejects_bad_epsilon():
    with pytest.raises(ValueError):
        EpsilonGreedyPolicy(actions=[Action("noop", ())], epsilon=-0.1)
    with pytest.raises(ValueError):
        EpsilonGreedyPolicy(actions=[Action("noop", ())], epsilon=1.1)


def test_epsilon_zero_is_purely_greedy():
    actions = [Action("noop", ()), Action("sortkey", ("a",))]
    pol = EpsilonGreedyPolicy(actions=actions, epsilon=0.0, rng=random.Random(0))
    pol.update(actions[1], 1.0)
    # With ε=0 the policy must pick the higher-mean arm deterministically.
    for _ in range(10):
        assert pol.choose() == actions[1]


def test_epsilon_one_is_uniform_random():
    actions = [Action("noop", ()), Action("sortkey", ("a",))]
    pol = EpsilonGreedyPolicy(actions=actions, epsilon=1.0, rng=random.Random(0))
    chosen = {repr(pol.choose()) for _ in range(50)}
    assert chosen == {repr(a) for a in actions}


def test_epsilon_greedy_converges():
    actions = [Action("noop", ()), Action("zorder", ("x", "y"))]
    pol = EpsilonGreedyPolicy(actions=actions, epsilon=0.1, rng=random.Random(1))
    rewards = {repr(actions[0]): 0.0, repr(actions[1]): 1.0}
    for _ in range(500):
        a = pol.choose()
        pol.update(a, rewards[repr(a)])
    assert pol._plays[repr(actions[1])] > pol._plays[repr(actions[0])]


# ------------------------------------------------------- Thompson


def test_thompson_rejects_non_positive_variance():
    with pytest.raises(ValueError):
        ThompsonPolicy(actions=[Action("noop", ())], prior_var=0)
    with pytest.raises(ValueError):
        ThompsonPolicy(actions=[Action("noop", ())], obs_var=-1)


def test_thompson_converges_to_best_arm():
    actions = [Action("noop", ()), Action("zorder", ("x", "y"))]
    pol = ThompsonPolicy(actions=actions, rng=random.Random(2))
    rewards = {repr(actions[0]): 0.0, repr(actions[1]): 1.0}
    for _ in range(500):
        a = pol.choose()
        pol.update(a, rewards[repr(a)])
    assert pol._plays[repr(actions[1])] > pol._plays[repr(actions[0])]
