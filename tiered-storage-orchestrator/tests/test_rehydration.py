"""Tests for RehydrationManager and SLA tracking."""
from __future__ import annotations

import pytest

from tiered_storage.schemas import RehydrationPriority, Tier, REHYDRATION_SLA_SECONDS
from tests.conftest import make_record


@pytest.mark.asyncio
async def test_enqueues_job(rehydration_manager, cold_tier):
    await cold_tier.put(make_record("rk1"))
    job = rehydration_manager.request_restore("rk1")

    assert job.key == "rk1"
    assert job.completed_at is None
    assert job.priority == RehydrationPriority.STANDARD


@pytest.mark.asyncio
async def test_deduplicate_in_flight(rehydration_manager, cold_tier):
    await cold_tier.put(make_record("rk2"))
    j1 = rehydration_manager.request_restore("rk2")
    j2 = rehydration_manager.request_restore("rk2")

    assert j1.job_id == j2.job_id  # same job returned


@pytest.mark.asyncio
async def test_restore_lands_on_warm(rehydration_manager, cold_tier, warm_tier):
    rec = make_record("rk3", {"payload": 42})
    await cold_tier.put(rec)

    rehydration_manager.request_restore("rk3")
    await rehydration_manager.run_once()

    warm_rec = await warm_tier.get("rk3")
    assert warm_rec is not None
    assert warm_rec.value == {"payload": 42}


@pytest.mark.asyncio
async def test_expedited_restore_lands_on_hot(rehydration_manager, cold_tier, hot_tier):
    rec = make_record("rk4", {"fast": True})
    await cold_tier.put(rec)

    rehydration_manager.request_restore("rk4", priority=RehydrationPriority.EXPEDITED, target_tier=Tier.HOT)
    await rehydration_manager.run_once()

    hot_rec = await hot_tier.get("rk4")
    assert hot_rec is not None


@pytest.mark.asyncio
async def test_sla_deadline_set_correctly(rehydration_manager, cold_tier):
    import time
    await cold_tier.put(make_record("rk5"))

    before = time.time()
    job = rehydration_manager.request_restore("rk5", priority=RehydrationPriority.EXPEDITED)
    after = time.time()

    expected_sla = REHYDRATION_SLA_SECONDS[RehydrationPriority.EXPEDITED]
    assert before + expected_sla <= job.sla_deadline <= after + expected_sla + 1


@pytest.mark.asyncio
async def test_sla_compliance_rate_100_pct(rehydration_manager, cold_tier):
    for i in range(3):
        await cold_tier.put(make_record(f"sla_{i}"))
        rehydration_manager.request_restore(f"sla_{i}", priority=RehydrationPriority.EXPEDITED)

    await rehydration_manager.run_once()

    report = rehydration_manager.sla_report()
    assert report["total_jobs"] == 3
    assert report["compliance_rate_pct"] == 100.0


@pytest.mark.asyncio
async def test_missing_cold_key_graceful(rehydration_manager):
    """Restoring a key that doesn't exist in cold should not raise."""
    rehydration_manager.request_restore("ghost_key")
    count = await rehydration_manager.run_once()
    assert count == 1  # processed without crash


@pytest.mark.asyncio
async def test_list_jobs(rehydration_manager, cold_tier):
    await cold_tier.put(make_record("j1"))
    await cold_tier.put(make_record("j2"))

    rehydration_manager.request_restore("j1")
    rehydration_manager.request_restore("j2")

    pending = rehydration_manager.list_jobs(completed=False)
    assert len(pending) == 2

    await rehydration_manager.run_once()

    done = rehydration_manager.list_jobs(completed=True)
    assert len(done) == 2
