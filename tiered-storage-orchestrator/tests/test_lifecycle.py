"""Tests for the lifecycle demotion engine."""
from __future__ import annotations

import time

import pytest

from tiered_storage.schemas import Tier
from tests.conftest import make_record


@pytest.mark.asyncio
async def test_fresh_keys_stay_hot(lifecycle_engine, hot_tier):
    fresh = make_record("fresh", age_days=0, access_count=10)
    await hot_tier.put(fresh)

    report = await lifecycle_engine.run_cycle()

    assert not any(m.key == "fresh" for m in report.hot_to_warm)
    assert await hot_tier.exists("fresh")


@pytest.mark.asyncio
async def test_idle_key_demoted_hot_to_warm(lifecycle_engine, hot_tier, warm_tier, policy):
    stale = make_record("idle", age_days=policy.hot_to_warm_idle_days + 1, access_count=0)
    stale.last_accessed_at = time.time() - (policy.hot_to_warm_idle_days + 1) * 86400
    await hot_tier.put(stale)

    report = await lifecycle_engine.run_cycle()

    assert any(m.key == "idle" for m in report.hot_to_warm)
    assert not await hot_tier.exists("idle")
    assert await warm_tier.exists("idle")


@pytest.mark.asyncio
async def test_idle_key_demoted_warm_to_cold(lifecycle_engine, warm_tier, cold_tier, policy):
    stale = make_record("w2c", age_days=policy.warm_to_cold_idle_days + 1, access_count=0)
    stale.last_accessed_at = time.time() - (policy.warm_to_cold_idle_days + 1) * 86400
    await warm_tier.put(stale)

    report = await lifecycle_engine.run_cycle()

    assert any(m.key == "w2c" for m in report.warm_to_cold)
    assert not await warm_tier.exists("w2c")
    assert await cold_tier.exists("w2c")


@pytest.mark.asyncio
async def test_cycle_report_summary(lifecycle_engine, hot_tier, policy):
    stale = make_record("s1", age_days=policy.hot_to_warm_idle_days + 1)
    stale.last_accessed_at = time.time() - (policy.hot_to_warm_idle_days + 1) * 86400
    await hot_tier.put(stale)

    report = await lifecycle_engine.run_cycle()
    summary = report.summary()

    assert "hot→warm" in summary
    assert str(len(report.hot_to_warm)) in summary


@pytest.mark.asyncio
async def test_history_retained(lifecycle_engine):
    await lifecycle_engine.run_cycle()
    await lifecycle_engine.run_cycle()

    assert len(lifecycle_engine.history()) == 2
    assert lifecycle_engine.last_cycle() is not None


@pytest.mark.asyncio
async def test_bytes_demoted_counted(lifecycle_engine, hot_tier, warm_tier, policy):
    stale = make_record("big", age_days=policy.hot_to_warm_idle_days + 1, access_count=0)
    stale.last_accessed_at = time.time() - (policy.hot_to_warm_idle_days + 1) * 86400
    stale.size_bytes = 50_000
    await hot_tier.put(stale)

    report = await lifecycle_engine.run_cycle()
    assert report.bytes_demoted >= 50_000
