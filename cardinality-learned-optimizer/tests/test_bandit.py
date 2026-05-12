"""Tests for Thompson Sampling bandit."""
import math
import pytest

from cle.bao.bandit import BayesianArm, ThompsonSamplingBandit


def test_bayesian_arm_update():
    arm = BayesianArm(prior_mean=5.0, prior_var=4.0)
    initial_var = arm.var
    arm.update(3.0)
    assert arm.n == 1
    assert arm.var < initial_var   # posterior variance contracts
    assert arm.mu != 5.0          # posterior mean shifts


def test_arm_sample_type():
    arm = BayesianArm()
    s = arm.sample()
    assert isinstance(s, float)


def test_bandit_select_returns_valid_arm():
    bandit = ThompsonSamplingBandit(num_arms=15)
    arm = bandit.select()
    assert 0 <= arm < 15


def test_bandit_exclude():
    bandit = ThompsonSamplingBandit(num_arms=5)
    # With all arms excluded except 2, must select 2
    arm = bandit.select(exclude=[0, 1, 3, 4])
    assert arm == 2


def test_bandit_update_records_history():
    bandit = ThompsonSamplingBandit(num_arms=5)
    bandit.update(2, 100.0)
    assert len(bandit.history) == 1
    assert bandit.history[0][0] == 2


def test_bandit_best_arm_shifts_with_updates():
    bandit = ThompsonSamplingBandit(num_arms=3)
    # Arm 1 gets fast observations
    for _ in range(20):
        bandit.update(1, 10.0)   # 10ms
    # Arm 0 gets slow observations
    for _ in range(20):
        bandit.update(0, 10_000.0)   # 10s
    best = bandit.best_arm()
    assert best == 1


def test_bandit_arm_stats():
    bandit = ThompsonSamplingBandit(num_arms=3)
    bandit.update(0, 50.0)
    stats = bandit.arm_stats()
    assert len(stats) == 3
    assert stats[0]["n"] == 1
    assert stats[1]["n"] == 0
