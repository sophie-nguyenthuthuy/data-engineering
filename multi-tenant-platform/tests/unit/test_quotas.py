"""Unit tests for the quota engine using a fake Redis."""
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.quotas.engine import QuotaEngine, QuotaCheckResult
from core.quotas.tiers import TIERS


class FakeScript:
    def __init__(self, return_value: int = 1) -> None:
        self._return = return_value

    async def __call__(self, keys, args) -> int:
        return self._return


class FakeRedis:
    def __init__(self) -> None:
        self._store: dict = {}

    def register_script(self, script: str):
        return FakeScript(return_value=1)

    async def get(self, key: str):
        return self._store.get(key)

    async def set(self, key: str, value, ex=None):
        self._store[key] = str(value)

    async def incrby(self, key: str, delta: int):
        current = int(self._store.get(key, 0))
        new = current + delta
        self._store[key] = str(new)
        return new

    async def incr(self, key: str):
        return await self.incrby(key, 1)

    async def decr(self, key: str):
        return await self.incrby(key, -1)

    async def expire(self, key: str, seconds: int):
        pass


@pytest.fixture
def redis_fake() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def engine(redis_fake: FakeRedis) -> QuotaEngine:
    return QuotaEngine(redis_fake)  # type: ignore


@pytest.mark.asyncio
async def test_request_quota_allowed(engine: QuotaEngine) -> None:
    tenant_id = uuid.uuid4()
    result = await engine.check_request(tenant_id, "starter")
    assert result.allowed is True
    assert result.dimension == "requests"


@pytest.mark.asyncio
async def test_request_quota_blocked(redis_fake: FakeRedis) -> None:
    engine = QuotaEngine(redis_fake)  # type: ignore
    engine._script = FakeScript(return_value=0)
    tenant_id = uuid.uuid4()
    result = await engine.check_request(tenant_id, "free")
    assert result.allowed is False


@pytest.mark.asyncio
async def test_job_quota_allows_within_limit(engine: QuotaEngine) -> None:
    tenant_id = uuid.uuid4()
    result = await engine.check_job(tenant_id, "free")
    assert result.allowed is True


@pytest.mark.asyncio
async def test_job_quota_blocks_at_limit(redis_fake: FakeRedis, engine: QuotaEngine) -> None:
    tenant_id = uuid.uuid4()
    key = f"quota:{tenant_id}:jobs"
    redis_fake._store[key] = str(TIERS["free"].concurrent_jobs)
    result = await engine.check_job(tenant_id, "free")
    assert result.allowed is False


@pytest.mark.asyncio
async def test_storage_quota_within_limit(engine: QuotaEngine) -> None:
    tenant_id = uuid.uuid4()
    result = await engine.check_storage(tenant_id, "free", 100)
    assert result.allowed is True


@pytest.mark.asyncio
async def test_storage_quota_exceeded(redis_fake: FakeRedis, engine: QuotaEngine) -> None:
    tenant_id = uuid.uuid4()
    redis_fake._store[f"storage:{tenant_id}:bytes"] = str(TIERS["free"].storage_bytes)
    result = await engine.check_storage(tenant_id, "free", 1)
    assert result.allowed is False


@pytest.mark.asyncio
async def test_enterprise_storage_always_allowed(engine: QuotaEngine) -> None:
    tenant_id = uuid.uuid4()
    result = await engine.check_storage(tenant_id, "enterprise", 10**12)
    assert result.allowed is True
