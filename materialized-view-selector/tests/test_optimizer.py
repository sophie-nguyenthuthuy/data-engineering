"""Tests for greedy + SA optimizer."""

import pytest
from mv_selector.models import CandidateView
from mv_selector.optimizer import AnnealingSelector, GreedySelector


def make_view(name: str, benefit: float, storage_mb: int, maintenance: float = 0.0) -> CandidateView:
    return CandidateView(
        sql=f"SELECT * FROM {name}",
        name=name,
        view_id=name,
        referenced_tables=[name],
        benefiting_query_ids=[],
        estimated_storage_bytes=storage_mb * 1024 * 1024,
        estimated_maintenance_cost_usd=maintenance,
        estimated_benefit_usd=benefit,
    )


CANDIDATES = [
    make_view("v_a", benefit=100.0, storage_mb=200),
    make_view("v_b", benefit=80.0,  storage_mb=100),
    make_view("v_c", benefit=60.0,  storage_mb=50),
    make_view("v_d", benefit=10.0,  storage_mb=400),
    make_view("v_e", benefit=5.0,   storage_mb=10),
    make_view("v_f", benefit=-1.0,  storage_mb=10),  # should never be picked
]

BUDGET = 300 * 1024 * 1024  # 300 MB


class TestGreedy:
    def test_respects_budget(self):
        result = GreedySelector().select(CANDIDATES, BUDGET)
        total = sum(v.estimated_storage_bytes for v in result.selected)
        assert total <= BUDGET

    def test_excludes_negative_benefit(self):
        result = GreedySelector().select(CANDIDATES, BUDGET)
        names = [v.name for v in result.selected]
        assert "v_f" not in names

    def test_no_candidates(self):
        result = GreedySelector().select([], BUDGET)
        assert result.selected == []

    def test_zero_budget(self):
        result = GreedySelector().select(CANDIDATES, 0)
        assert result.selected == []

    def test_benefit_is_positive(self):
        result = GreedySelector().select(CANDIDATES, BUDGET)
        assert result.total_estimated_benefit_usd > 0


class TestAnnealingSelector:
    def test_respects_budget(self):
        sa = AnnealingSelector(seed=42, max_iterations=5_000)
        result = sa.select(CANDIDATES, BUDGET)
        total = sum(v.estimated_storage_bytes for v in result.selected)
        assert total <= BUDGET

    def test_excludes_negative_benefit(self):
        sa = AnnealingSelector(seed=42, max_iterations=5_000)
        result = sa.select(CANDIDATES, BUDGET)
        names = [v.name for v in result.selected]
        assert "v_f" not in names

    def test_no_worse_than_greedy(self):
        budget = 500 * 1024 * 1024
        greedy_result = GreedySelector().select(CANDIDATES, budget)
        sa = AnnealingSelector(seed=7, max_iterations=10_000)
        sa_result = sa.select(CANDIDATES, budget)
        assert sa_result.net_benefit_usd >= greedy_result.net_benefit_usd - 0.01

    def test_empty_candidates(self):
        sa = AnnealingSelector(seed=0)
        result = sa.select([], BUDGET)
        assert result.selected == []

    def test_history_nondecreasing(self):
        sa = AnnealingSelector(seed=1, max_iterations=10_000)
        result = sa.select(CANDIDATES, BUDGET)
        for a, b in zip(result.incumbent_history, result.incumbent_history[1:]):
            assert b >= a - 1e-9

    def test_result_metadata(self):
        sa = AnnealingSelector(seed=2, max_iterations=1_000)
        result = sa.select(CANDIDATES, BUDGET)
        assert result.algorithm == "annealing"
        assert result.elapsed_seconds >= 0
        assert result.iterations > 0
