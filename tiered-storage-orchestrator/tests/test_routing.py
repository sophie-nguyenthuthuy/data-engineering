"""Tests for the transparent read router."""
from __future__ import annotations

import pytest

from tiered_storage.schemas import Tier
from tests.conftest import make_record


@pytest.mark.asyncio
async def test_hot_hit(router, hot_tier):
    rec = make_record("k1", {"x": 1})
    await hot_tier.put(rec)

    result = await router.get("k1")

    assert result.tier_hit == Tier.HOT
    assert result.record is not None
    assert result.record.value == {"x": 1}
    assert router.stats.hot_hits == 1


@pytest.mark.asyncio
async def test_warm_hit(router, warm_tier):
    rec = make_record("k2", {"y": 2})
    await warm_tier.put(rec)

    result = await router.get("k2")

    assert result.tier_hit == Tier.WARM
    assert result.record.value == {"y": 2}
    assert router.stats.warm_hits == 1


@pytest.mark.asyncio
async def test_cold_triggers_rehydration(router, cold_tier):
    rec = make_record("k3", {"z": 3})
    await cold_tier.put(rec)

    result = await router.get("k3")

    assert result.tier_hit == Tier.COLD
    assert result.record is None          # non-blocking: not restored yet
    assert result.rehydration_job is not None
    assert router.stats.cold_misses == 1


@pytest.mark.asyncio
async def test_cold_blocking_restore(router, cold_tier, rehydration_manager):
    rec = make_record("k4", {"w": 4})
    await cold_tier.put(rec)

    # Non-blocking get enqueues the job; drain the queue; then verify warm tier
    result = await router.get("k4", block_on_cold=False)
    assert result.rehydration_job is not None

    await rehydration_manager.run_once()

    # Record should now be in warm tier
    warm_rec = await router._warm.get("k4")
    assert warm_rec is not None
    assert warm_rec.value == {"w": 4}


@pytest.mark.asyncio
async def test_not_found(router):
    result = await router.get("nonexistent")
    assert result.tier_hit == Tier.UNKNOWN
    assert result.record is None
    assert result.rehydration_job is None
    assert router.stats.total_misses == 1


@pytest.mark.asyncio
async def test_warm_to_hot_promotion(router, warm_tier):
    """A warm key accessed frequently enough should be promoted to hot."""
    rec = make_record("k5", {"promo": True})
    await warm_tier.put(rec)

    # Hit the key enough times to push EMA freq above threshold (3.0)
    for _ in range(10):
        await router.get("k5")

    assert router.stats.promotions >= 1
    # Key should now be on hot tier
    hot_rec = await router._hot.get("k5")
    assert hot_rec is not None


@pytest.mark.asyncio
async def test_locate(router, hot_tier, warm_tier, cold_tier):
    await hot_tier.put(make_record("h"))
    await warm_tier.put(make_record("w"))
    await cold_tier.put(make_record("c"))

    assert await router.locate("h") == Tier.HOT
    assert await router.locate("w") == Tier.WARM
    assert await router.locate("c") == Tier.COLD
    assert await router.locate("gone") == Tier.UNKNOWN


@pytest.mark.asyncio
async def test_hit_rate_tracking(router, hot_tier, warm_tier):
    await hot_tier.put(make_record("a"))
    await warm_tier.put(make_record("b"))

    await router.get("a")
    await router.get("b")
    await router.get("missing")

    assert router.stats.total_reads == 3
    assert router.stats.hit_rate == pytest.approx(2 / 3)
