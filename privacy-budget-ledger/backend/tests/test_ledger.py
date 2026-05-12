"""
Tests for the unified composition ledger.
"""
import math
import pytest

from app.composition.ledger import (
    CompositionLedger,
    BudgetAllocationSpec,
    make_query_cost_gaussian,
    make_query_cost_laplace,
)
from app.composition.zcdp import zcdp_to_dp


def make_ledger(total_epsilon: float = 5.0, total_delta: float = 1e-5) -> CompositionLedger:
    spec = BudgetAllocationSpec(
        total_epsilon=total_epsilon,
        total_delta=total_delta,
    )
    return CompositionLedger(spec)


class TestLedgerInitial:
    def test_zero_consumption(self):
        ledger = make_ledger()
        assert ledger.consumed_dp_epsilon_basic() == 0.0
        assert ledger.consumed_dp_epsilon_rdp() == 0.0
        assert ledger.consumed_dp_epsilon_zcdp() == 0.0

    def test_remaining_equals_total(self):
        ledger = make_ledger(total_epsilon=3.0)
        assert ledger.remaining_budget_rdp() == pytest.approx(3.0, abs=1e-6)


class TestLedgerCommit:
    def test_commit_increases_basic(self):
        ledger = make_ledger()
        cost = make_query_cost_gaussian(1.0, 1.0, 1e-5)
        ledger.commit_query(cost)
        assert ledger.state.consumed_epsilon_basic == pytest.approx(cost.epsilon_basic)

    def test_commit_increases_rho(self):
        ledger = make_ledger()
        cost = make_query_cost_gaussian(1.0, 1.0, 1e-5)
        ledger.commit_query(cost)
        assert ledger.state.consumed_rho == pytest.approx(cost.rho)

    def test_composition_subadditive(self):
        # After k identical Gaussian queries, RDP ε < k × single-query ε
        ledger = make_ledger(total_epsilon=100.0)
        cost = make_query_cost_gaussian(1.0, 1.0, 1e-5)
        single_rdp = ledger.consumed_dp_epsilon_rdp()  # 0

        for _ in range(10):
            ledger.commit_query(cost)

        after_10 = ledger.consumed_dp_epsilon_rdp()
        single_eps = cost.epsilon_basic
        assert after_10 < 10 * single_eps  # tighter than basic composition

    def test_savings_positive_after_queries(self):
        ledger = make_ledger(total_epsilon=100.0)
        cost = make_query_cost_gaussian(1.0, 1.0, 1e-5)
        for _ in range(5):
            ledger.commit_query(cost)
        assert ledger.savings_vs_basic() > 0


class TestLedgerPlan:
    def test_accept_within_budget(self):
        ledger = make_ledger(total_epsilon=5.0)
        cost = make_query_cost_gaussian(1.0, 2.0, 1e-5)  # small sigma → modest ε
        plan = ledger.plan_query(cost)
        assert plan.decision == "accept"

    def test_reject_exceeds_budget(self):
        # Give a very small budget, try a large query
        ledger = make_ledger(total_epsilon=0.01)
        cost = make_query_cost_gaussian(1.0, 0.1, 1e-5)  # tiny σ → large ε
        plan = ledger.plan_query(cost)
        assert plan.decision in ("rewrite", "reject")

    def test_rewrite_has_feasible_eps(self):
        # Budget that can only support moderate noise
        ledger = make_ledger(total_epsilon=1.0)
        cost = make_query_cost_gaussian(1.0, 0.5, 1e-5)  # aggressive ε
        plan = ledger.plan_query(cost)
        if plan.decision == "rewrite":
            assert plan.epsilon_feasible is not None
            assert plan.epsilon_feasible < plan.epsilon_requested
            assert plan.sigma_feasible is not None
            assert plan.sigma_feasible > 0.5  # more noise

    def test_plan_does_not_mutate_state(self):
        ledger = make_ledger(total_epsilon=5.0)
        cost = make_query_cost_gaussian(1.0, 1.0, 1e-5)
        before_rho = ledger.state.consumed_rho
        ledger.plan_query(cost)
        assert ledger.state.consumed_rho == before_rho

    def test_basic_vs_tight_savings_in_plan(self):
        # After several queries, a new query's basic projection > tight projection
        ledger = make_ledger(total_epsilon=100.0)
        cost = make_query_cost_gaussian(1.0, 1.0, 1e-5)
        for _ in range(20):
            ledger.commit_query(cost)
        plan = ledger.plan_query(cost)
        if plan.decision == "accept":
            assert plan.projected_epsilon_basic > plan.projected_epsilon_rdp


class TestLedgerLaplace:
    def test_laplace_cost_accept(self):
        ledger = make_ledger(total_epsilon=5.0)
        cost = make_query_cost_laplace(1.0, 0.5)
        plan = ledger.plan_query(cost)
        assert plan.decision in ("accept", "rewrite")

    def test_laplace_commit(self):
        ledger = make_ledger(total_epsilon=5.0)
        cost = make_query_cost_laplace(1.0, 0.5)
        ledger.commit_query(cost)
        assert ledger.state.consumed_epsilon_basic == pytest.approx(0.5)
