"""Tests for the cost model and savings calculations."""
from __future__ import annotations

import pytest

from tiered_storage.cost_model import CostConfig, CostModel, TierUsage
from tiered_storage.schemas import RehydrationPriority, Tier, TierMetrics


def make_metrics(tier: Tier, size_gb: float = 1.0, records: int = 100) -> TierMetrics:
    return TierMetrics(
        tier=tier,
        record_count=records,
        total_size_bytes=int(size_gb * 1e9),
        avg_access_frequency=1.0,
        oldest_record_age_days=10.0,
        newest_record_age_days=0.5,
    )


@pytest.fixture
def model():
    return CostModel(CostConfig())


def test_total_is_sum_of_parts(model):
    usage = TierUsage(
        redis_used_gb=5.0,
        postgres_used_gb=5.0,
        hot_reads_per_day=1000,
        warm_used_gb=100.0,
        warm_reads_per_day=200,
        cold_used_gb=500.0,
        cold_reads_per_day=5,
    )
    breakdown = model.monthly_cost(usage)
    expected = (
        breakdown.hot_redis_usd
        + breakdown.hot_postgres_usd
        + breakdown.warm_s3_usd
        + breakdown.cold_archive_usd
        + breakdown.rehydration_usd
    )
    assert breakdown.total_usd == pytest.approx(expected, rel=1e-6)


def test_cold_cheaper_than_warm(model):
    usage_warm = TierUsage(warm_used_gb=100.0)
    usage_cold = TierUsage(cold_used_gb=100.0)
    warm_cost = model.monthly_cost(usage_warm).warm_s3_usd
    cold_cost = model.monthly_cost(usage_cold).cold_archive_usd
    assert cold_cost < warm_cost


def test_expedited_rehydration_costs_more(model):
    base = TierUsage(cold_reads_per_day=100)
    base.cold_read_priority = RehydrationPriority.EXPEDITED
    expensive = model.monthly_cost(base).rehydration_usd

    base.cold_read_priority = RehydrationPriority.BULK
    cheap = model.monthly_cost(base).rehydration_usd

    assert expensive > cheap


def test_project_from_metrics(model):
    hot_m  = make_metrics(Tier.HOT,  size_gb=2.0)
    warm_m = make_metrics(Tier.WARM, size_gb=50.0)
    cold_m = make_metrics(Tier.COLD, size_gb=200.0)

    breakdown = model.project_from_metrics(hot_m, warm_m, cold_m)
    assert breakdown.total_usd > 0
    # Cold storage is large — should contribute meaningfully
    assert breakdown.cold_archive_usd > 0


def test_savings_from_demotion_positive(model):
    savings = model.savings_from_demotion(10.0, from_tier="hot_postgres", to_tier="warm")
    assert savings > 0


def test_savings_from_demotion_impossible(model):
    """Moving from cold to hot costs more — savings should be negative."""
    savings = model.savings_from_demotion(10.0, from_tier="cold", to_tier="hot_postgres")
    assert savings < 0


def test_breakeven_days_finite(model):
    days = model.breakeven_days(100.0, "hot_postgres", "warm")
    assert 0 < days < 3650  # reasonable range


def test_breakeven_days_infinite_when_no_savings(model):
    days = model.breakeven_days(10.0, "cold", "hot_postgres")
    assert days == float("inf")


def test_summary_contains_total(model):
    usage = TierUsage(redis_used_gb=1.0, warm_used_gb=10.0, cold_used_gb=50.0)
    breakdown = model.monthly_cost(usage)
    summary = breakdown.summary()
    assert "TOTAL" in summary
    assert str(round(breakdown.total_usd, 2)) in summary or "$" in summary


def test_zero_usage(model):
    breakdown = model.monthly_cost(TierUsage())
    # Postgres instance cost is always present even with 0 GB
    assert breakdown.hot_postgres_usd > 0
    assert breakdown.warm_s3_usd == pytest.approx(0.0, abs=0.01)
    assert breakdown.cold_archive_usd == pytest.approx(0.0, abs=0.01)
