"""Integration-style tests for the full orchestrator stack (no real services)."""
from __future__ import annotations

import time

import pytest

from tiered_storage.schemas import RehydrationPriority, Tier
from tests.conftest import make_record


@pytest.mark.asyncio
async def test_put_and_get(orchestrator):
    await orchestrator.put("user:1", {"name": "Alice"})
    result = await orchestrator.get("user:1")

    assert result.record is not None
    assert result.record.value == {"name": "Alice"}
    assert result.tier_hit == Tier.HOT


@pytest.mark.asyncio
async def test_get_missing_key(orchestrator):
    result = await orchestrator.get("does_not_exist")
    assert result.record is None
    assert result.tier_hit == Tier.UNKNOWN


@pytest.mark.asyncio
async def test_delete_removes_from_all_tiers(orchestrator, hot_tier, warm_tier, cold_tier):
    await orchestrator.put("todelete", {"x": 1})
    # Also plant copies on warm and cold
    await warm_tier.put(make_record("todelete"))
    await cold_tier.put(make_record("todelete"))

    found = await orchestrator.delete("todelete")
    assert found is True

    assert not await hot_tier.exists("todelete")
    assert not await warm_tier.exists("todelete")
    assert not await cold_tier.exists("todelete")


@pytest.mark.asyncio
async def test_locate(orchestrator, warm_tier):
    await orchestrator.put("hot_key", {"tier": "hot"})
    await warm_tier.put(make_record("warm_key"))

    assert await orchestrator.locate("hot_key") == Tier.HOT
    assert await orchestrator.locate("warm_key") == Tier.WARM
    assert await orchestrator.locate("nowhere") == Tier.UNKNOWN


@pytest.mark.asyncio
async def test_lifecycle_demotes_stale_keys(orchestrator, hot_tier, warm_tier):
    """A stale record should move hot → warm after one lifecycle cycle."""
    stale = make_record("stale_key", age_days=10, access_count=0)
    stale.last_accessed_at = time.time() - 10 * 86400  # 10 days ago
    await hot_tier.put(stale)

    report = await orchestrator.run_lifecycle_cycle()

    assert any(m.key == "stale_key" for m in report.hot_to_warm), (
        f"Expected stale_key in hot_to_warm, got: {[m.key for m in report.hot_to_warm]}"
    )
    assert not await hot_tier.exists("stale_key")
    assert await warm_tier.exists("stale_key")


@pytest.mark.asyncio
async def test_rehydrate_cold_key(orchestrator, cold_tier):
    rec = make_record("cold_key", {"tier": "cold"})
    await cold_tier.put(rec)

    job = await orchestrator.rehydrate(
        "cold_key",
        priority=RehydrationPriority.EXPEDITED,
        block=False,
    )
    assert job is not None
    assert job.key == "cold_key"

    # Drain the rehydration queue
    processed = await orchestrator.rehydration.run_once()
    assert processed >= 1

    warm_rec = await orchestrator.warm.get("cold_key")
    assert warm_rec is not None
    assert warm_rec.value == {"tier": "cold"}


@pytest.mark.asyncio
async def test_sla_met_on_fast_restore(orchestrator, cold_tier):
    rec = make_record("sla_key")
    await cold_tier.put(rec)

    job = await orchestrator.rehydrate("sla_key", priority=RehydrationPriority.EXPEDITED)
    await orchestrator.rehydration.run_once()

    assert job.completed_at is not None
    assert job.sla_met, f"SLA violated: completed={job.completed_at:.2f} deadline={job.sla_deadline:.2f}"


@pytest.mark.asyncio
async def test_cost_report_returns_positive(orchestrator):
    await orchestrator.put("k1", {"v": 1})
    breakdown = await orchestrator.cost_report()

    # Postgres instance is always present
    assert breakdown.hot_postgres_usd > 0
    assert breakdown.total_usd > 0


@pytest.mark.asyncio
async def test_metrics_structure(orchestrator):
    await orchestrator.put("m1", {"v": 1})
    m = await orchestrator.metrics()

    assert "tiers" in m
    assert "router" in m
    assert "rehydration" in m
    assert "lifecycle" in m
    assert m["tiers"]["hot"]["records"] >= 1


@pytest.mark.asyncio
async def test_access_tracker_records_reads(orchestrator):
    await orchestrator.put("tracked", {"v": 99})

    for _ in range(5):
        await orchestrator.get("tracked")

    stats = orchestrator.tracker.get("tracked")
    assert stats is not None
    assert stats.access_count >= 5


@pytest.mark.asyncio
async def test_savings_report(orchestrator, warm_tier, cold_tier):
    # Populate warm and cold with some data
    for i in range(3):
        r = make_record(f"warm_{i}", size_bytes=1_000_000)
        await warm_tier.put(r)
        r2 = make_record(f"cold_{i}", size_bytes=5_000_000)
        await cold_tier.put(r2)

    report = await orchestrator.savings_report()
    assert "warm_vs_hot_savings_usd" in report
    assert "cold_vs_warm_savings_usd" in report
    assert report["warm_vs_hot_savings_usd"] >= 0
