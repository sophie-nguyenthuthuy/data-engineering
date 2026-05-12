"""
Token-bucket quota engine backed by Redis.

Each tenant gets a bucket per quota dimension (requests, jobs).
Buckets are implemented as a Lua script for atomicity — no TOCTOU race.
"""
from dataclasses import dataclass
from uuid import UUID

import redis.asyncio as aioredis

from .tiers import TIERS, TierLimits


_LUA_TOKEN_BUCKET = """
local key        = KEYS[1]
local capacity   = tonumber(ARGV[1])
local refill_per_sec = tonumber(ARGV[2])
local now        = tonumber(ARGV[3])
local requested  = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens     = tonumber(data[1]) or capacity
local last_refill = tonumber(data[2]) or now

local elapsed = math.max(0, now - last_refill)
tokens = math.min(capacity, tokens + elapsed * refill_per_sec)

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return 1
else
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return 0
end
"""


class QuotaExceeded(Exception):
    def __init__(self, dimension: str, tenant_id: UUID) -> None:
        super().__init__(f"Quota exceeded: {dimension} for tenant {tenant_id}")
        self.dimension = dimension
        self.tenant_id = tenant_id


@dataclass
class QuotaCheckResult:
    allowed: bool
    dimension: str
    tenant_id: UUID


class QuotaEngine:
    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client
        self._script = redis_client.register_script(_LUA_TOKEN_BUCKET)

    def _tier(self, tier: str) -> TierLimits:
        return TIERS.get(tier, TIERS["free"])

    def _bucket_key(self, tenant_id: UUID, dimension: str) -> str:
        return f"quota:{tenant_id}:{dimension}"

    async def check_request(self, tenant_id: UUID, tier: str) -> QuotaCheckResult:
        limits = self._tier(tier)
        import time
        now = time.time()
        refill_per_sec = limits.requests_per_minute / 60.0

        allowed = await self._script(
            keys=[self._bucket_key(tenant_id, "requests")],
            args=[limits.burst_requests, refill_per_sec, now, 1],
        )
        return QuotaCheckResult(allowed=bool(allowed), dimension="requests", tenant_id=tenant_id)

    async def check_job(self, tenant_id: UUID, tier: str) -> QuotaCheckResult:
        """Check and reserve a concurrent job slot."""
        limits = self._tier(tier)
        key = self._bucket_key(tenant_id, "jobs")
        current = int(await self._redis.get(key) or 0)
        if current >= limits.concurrent_jobs:
            return QuotaCheckResult(allowed=False, dimension="jobs", tenant_id=tenant_id)
        await self._redis.incr(key)
        await self._redis.expire(key, 3600)
        return QuotaCheckResult(allowed=True, dimension="jobs", tenant_id=tenant_id)

    async def release_job(self, tenant_id: UUID) -> None:
        key = self._bucket_key(tenant_id, "jobs")
        val = int(await self._redis.get(key) or 0)
        if val > 0:
            await self._redis.decr(key)

    async def get_storage_used(self, tenant_id: UUID) -> int:
        """Return bytes used (stored separately by the object storage layer)."""
        key = f"storage:{tenant_id}:bytes"
        return int(await self._redis.get(key) or 0)

    async def add_storage_bytes(self, tenant_id: UUID, delta: int) -> int:
        key = f"storage:{tenant_id}:bytes"
        return int(await self._redis.incrby(key, delta))

    async def check_storage(self, tenant_id: UUID, tier: str, incoming_bytes: int) -> QuotaCheckResult:
        limits = self._tier(tier)
        if limits.storage_bytes == -1:
            return QuotaCheckResult(allowed=True, dimension="storage", tenant_id=tenant_id)
        used = await self.get_storage_used(tenant_id)
        allowed = (used + incoming_bytes) <= limits.storage_bytes
        return QuotaCheckResult(allowed=allowed, dimension="storage", tenant_id=tenant_id)
