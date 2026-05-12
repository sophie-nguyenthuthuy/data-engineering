"""
Shared fixtures — uses in-process fakes so tests need no real Redis/Postgres/S3.

  FakeHotTier  — dict-backed hot tier
  FakeWarmTier — dict-backed warm tier (no S3)
  FakeColdTier — uses ColdTier with a tmp local directory
"""
from __future__ import annotations

import tempfile
import time
from typing import Optional

import pytest
import pytest_asyncio

from tiered_storage.config import StorageConfig
from tiered_storage.cost_model import CostConfig
from tiered_storage.lifecycle import LifecycleEngine
from tiered_storage.orchestrator import TieredStorageOrchestrator
from tiered_storage.rehydration import RehydrationManager
from tiered_storage.router import ReadRouter
from tiered_storage.schemas import DataRecord, LifecyclePolicy, Tier, TierMetrics
from tiered_storage.tiers.base import BaseTier
from tiered_storage.tiers.cold import ColdTier
from tiered_storage.tracking.access_patterns import AccessPatternTracker


# ── Fake tiers ────────────────────────────────────────────────────────────

class FakeTier(BaseTier):
    """Dict-backed tier for testing."""

    def __init__(self, tier: Tier = Tier.HOT):
        self._store: dict[str, DataRecord] = {}
        self._tier = tier

    async def get(self, key: str) -> Optional[DataRecord]:
        rec = self._store.get(key)
        if rec:
            rec.last_accessed_at = time.time()
            rec.access_count += 1
        return rec

    async def put(self, record: DataRecord) -> None:
        record.tier = self._tier
        self._store[record.key] = record

    async def delete(self, key: str) -> bool:
        return self._store.pop(key, None) is not None

    async def exists(self, key: str) -> bool:
        return key in self._store

    async def list_keys(self, prefix: str = "", limit: int = 1000) -> list[str]:
        return [k for k in self._store if k.startswith(prefix)][:limit]

    async def metrics(self) -> TierMetrics:
        now = time.time()
        records = list(self._store.values())
        total_bytes = sum(r.size_bytes for r in records)
        ages = [(now - r.created_at) / 86400 for r in records] or [0.0]
        freqs = [r.access_count / max(a, 1) for r, a in zip(records, ages)] or [0.0]
        return TierMetrics(
            tier=self._tier,
            record_count=len(records),
            total_size_bytes=total_bytes,
            avg_access_frequency=sum(freqs) / max(len(freqs), 1),
            oldest_record_age_days=max(ages),
            newest_record_age_days=min(ages),
        )

    async def get_stale_keys(self, idle_days: float, min_freq: float) -> list[str]:
        now = time.time()
        cutoff = now - idle_days * 86400
        stale = []
        for key, rec in self._store.items():
            if rec.last_accessed_at < cutoff:
                stale.append(key)
                continue
            age = max((now - rec.created_at) / 86400, 1)
            if rec.access_count / age < min_freq:
                stale.append(key)
        return stale


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def hot_tier():
    return FakeTier(Tier.HOT)


@pytest.fixture
def warm_tier():
    return FakeTier(Tier.WARM)


@pytest.fixture
def cold_tier(tmp_path):
    t = ColdTier(local_path=str(tmp_path / "cold"))
    t.connect()
    return t


@pytest.fixture
def tracker():
    return AccessPatternTracker()


@pytest.fixture
def rehydration_manager(cold_tier, warm_tier, hot_tier):
    return RehydrationManager(
        cold_tier=cold_tier,
        warm_tier=warm_tier,
        hot_tier=hot_tier,
    )


@pytest.fixture
def router(hot_tier, warm_tier, cold_tier, rehydration_manager, tracker):
    return ReadRouter(
        hot_tier=hot_tier,
        warm_tier=warm_tier,
        cold_tier=cold_tier,
        rehydration_manager=rehydration_manager,
        tracker=tracker,
        promote_freq_threshold=3.0,
        block_on_cold=False,
    )


@pytest.fixture
def policy():
    return LifecyclePolicy(
        hot_to_warm_idle_days=1,
        warm_to_cold_idle_days=2,
        hot_min_access_freq=0.5,
        warm_min_access_freq=0.1,
    )


@pytest.fixture
def lifecycle_engine(policy, hot_tier, warm_tier, cold_tier, tracker):
    return LifecycleEngine(
        policy=policy,
        hot_tier=hot_tier,
        warm_tier=warm_tier,
        cold_tier=cold_tier,
        tracker=tracker,
    )


@pytest_asyncio.fixture
async def orchestrator(tmp_path, hot_tier, warm_tier, cold_tier):
    cfg = StorageConfig(
        cold_local_path=str(tmp_path / "cold"),
        lifecycle_interval_seconds=9999,
    )
    orch = TieredStorageOrchestrator(
        config=cfg,
        hot_tier=hot_tier,
        warm_tier=warm_tier,
        cold_tier=cold_tier,
    )
    await orch.start(run_lifecycle=False)
    yield orch
    await orch.stop()


def make_record(
    key: str,
    value=None,
    age_days: float = 0,
    access_count: int = 1,
    size_bytes: int = 100,
) -> DataRecord:
    now = time.time()
    return DataRecord(
        key=key,
        value=value or {"data": key},
        size_bytes=size_bytes,
        created_at=now - age_days * 86400,
        last_accessed_at=now - age_days * 86400,
        access_count=access_count,
    )
