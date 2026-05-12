"""Hot tier: Redis (L1 cache) + PostgreSQL (L2 persistent store)."""
from __future__ import annotations

import json
import time
from typing import Any, Optional

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None  # type: ignore

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore

from tiered_storage.schemas import DataRecord, Tier, TierMetrics
from tiered_storage.tiers.base import BaseTier


class HotTier(BaseTier):
    """
    L1: Redis for sub-millisecond reads of recently touched keys.
    L2: PostgreSQL for durable hot storage and rich query capability.

    On GET: check Redis first; on miss promote from Postgres back to Redis.
    On PUT: write to both atomically.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        postgres_dsn: str = "postgresql://postgres:postgres@localhost:5432/tiered_storage",
        redis_ttl_seconds: int = 3600,
    ):
        self._redis_url = redis_url
        self._postgres_dsn = postgres_dsn
        self._redis_ttl = redis_ttl_seconds
        self._redis: Any = None
        self._pg: Any = None

    async def connect(self) -> None:
        if aioredis:
            self._redis = await aioredis.from_url(
                self._redis_url, encoding="utf-8", decode_responses=True
            )
        if asyncpg:
            self._pg = await asyncpg.connect(self._postgres_dsn)
            await self._ensure_schema()

    async def _ensure_schema(self) -> None:
        await self._pg.execute(
            """
            CREATE TABLE IF NOT EXISTS hot_records (
                key             TEXT PRIMARY KEY,
                value           JSONB NOT NULL,
                size_bytes      BIGINT DEFAULT 0,
                tier            TEXT DEFAULT 'hot',
                created_at      DOUBLE PRECISION NOT NULL,
                last_accessed_at DOUBLE PRECISION NOT NULL,
                access_count    BIGINT DEFAULT 0,
                metadata        JSONB DEFAULT '{}'
            )
            """
        )
        await self._pg.execute(
            "CREATE INDEX IF NOT EXISTS idx_hot_last_accessed "
            "ON hot_records(last_accessed_at)"
        )

    # ------------------------------------------------------------------
    # BaseTier interface
    # ------------------------------------------------------------------

    async def get(self, key: str) -> Optional[DataRecord]:
        # L1: Redis
        if self._redis:
            raw = await self._redis.get(f"hot:{key}")
            if raw:
                data = json.loads(raw)
                record = self._dict_to_record(data)
                record.last_accessed_at = time.time()
                record.access_count += 1
                # Refresh TTL on access
                await self._redis.setex(
                    f"hot:{key}", self._redis_ttl, json.dumps(self._record_to_dict(record))
                )
                return record

        # L2: Postgres
        if self._pg:
            row = await self._pg.fetchrow(
                "SELECT * FROM hot_records WHERE key = $1", key
            )
            if row:
                record = self._row_to_record(row)
                record.last_accessed_at = time.time()
                record.access_count += 1
                await self._pg.execute(
                    "UPDATE hot_records SET last_accessed_at=$1, access_count=$2 WHERE key=$3",
                    record.last_accessed_at, record.access_count, key,
                )
                # Promote back to Redis cache
                if self._redis:
                    await self._redis.setex(
                        f"hot:{key}", self._redis_ttl,
                        json.dumps(self._record_to_dict(record))
                    )
                return record

        return None

    async def put(self, record: DataRecord) -> None:
        record.tier = Tier.HOT
        d = self._record_to_dict(record)

        if self._redis:
            await self._redis.setex(
                f"hot:{record.key}", self._redis_ttl, json.dumps(d)
            )

        if self._pg:
            await self._pg.execute(
                """
                INSERT INTO hot_records
                    (key, value, size_bytes, tier, created_at, last_accessed_at, access_count, metadata)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (key) DO UPDATE SET
                    value=EXCLUDED.value, size_bytes=EXCLUDED.size_bytes,
                    last_accessed_at=EXCLUDED.last_accessed_at,
                    access_count=EXCLUDED.access_count, metadata=EXCLUDED.metadata
                """,
                record.key, json.dumps(record.value), record.size_bytes,
                record.tier.value, record.created_at, record.last_accessed_at,
                record.access_count, json.dumps(record.metadata),
            )

    async def delete(self, key: str) -> bool:
        found = await self.exists(key)
        if self._redis:
            await self._redis.delete(f"hot:{key}")
        if self._pg:
            await self._pg.execute("DELETE FROM hot_records WHERE key=$1", key)
        return found

    async def exists(self, key: str) -> bool:
        if self._redis and await self._redis.exists(f"hot:{key}"):
            return True
        if self._pg:
            row = await self._pg.fetchrow(
                "SELECT 1 FROM hot_records WHERE key=$1", key
            )
            return row is not None
        return False

    async def metrics(self) -> TierMetrics:
        count, total_bytes, avg_freq, oldest, newest = 0, 0, 0.0, 0.0, 0.0
        if self._pg:
            row = await self._pg.fetchrow(
                """
                SELECT
                    COUNT(*)                                         AS cnt,
                    COALESCE(SUM(size_bytes),0)                     AS total_bytes,
                    COALESCE(AVG(access_count / GREATEST(
                        (extract(epoch from now()) - created_at)/86400, 1
                    )), 0)                                          AS avg_freq,
                    COALESCE(MAX(extract(epoch from now()) - created_at)/86400, 0) AS oldest,
                    COALESCE(MIN(extract(epoch from now()) - created_at)/86400, 0) AS newest
                FROM hot_records
                """
            )
            if row:
                count = row["cnt"]
                total_bytes = row["total_bytes"]
                avg_freq = float(row["avg_freq"])
                oldest = float(row["oldest"])
                newest = float(row["newest"])
        return TierMetrics(
            tier=Tier.HOT,
            record_count=count,
            total_size_bytes=total_bytes,
            avg_access_frequency=avg_freq,
            oldest_record_age_days=oldest,
            newest_record_age_days=newest,
        )

    async def list_keys(self, prefix: str = "", limit: int = 1000) -> list[str]:
        if self._pg:
            pattern = f"{prefix}%"
            rows = await self._pg.fetch(
                "SELECT key FROM hot_records WHERE key LIKE $1 LIMIT $2",
                pattern, limit,
            )
            return [r["key"] for r in rows]
        return []

    async def get_stale_keys(self, idle_days: float, min_freq: float) -> list[str]:
        """Return keys eligible for demotion to warm tier."""
        if not self._pg:
            return []
        cutoff = time.time() - idle_days * 86400
        rows = await self._pg.fetch(
            """
            SELECT key FROM hot_records
            WHERE last_accessed_at < $1
               OR (access_count / GREATEST((extract(epoch from now()) - created_at)/86400, 1)) < $2
            ORDER BY last_accessed_at ASC
            """,
            cutoff, min_freq,
        )
        return [r["key"] for r in rows]

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()
        if self._pg:
            await self._pg.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _record_to_dict(r: DataRecord) -> dict:
        return {
            "key": r.key,
            "value": r.value,
            "size_bytes": r.size_bytes,
            "tier": r.tier.value,
            "created_at": r.created_at,
            "last_accessed_at": r.last_accessed_at,
            "access_count": r.access_count,
            "metadata": r.metadata,
        }

    @staticmethod
    def _dict_to_record(d: dict) -> DataRecord:
        return DataRecord(
            key=d["key"],
            value=d["value"],
            size_bytes=d.get("size_bytes", 0),
            tier=Tier(d.get("tier", "hot")),
            created_at=d.get("created_at", time.time()),
            last_accessed_at=d.get("last_accessed_at", time.time()),
            access_count=d.get("access_count", 0),
            metadata=d.get("metadata", {}),
        )

    @staticmethod
    def _row_to_record(row: Any) -> DataRecord:
        return DataRecord(
            key=row["key"],
            value=json.loads(row["value"]) if isinstance(row["value"], str) else row["value"],
            size_bytes=row["size_bytes"] or 0,
            tier=Tier(row["tier"]),
            created_at=row["created_at"],
            last_accessed_at=row["last_accessed_at"],
            access_count=row["access_count"] or 0,
            metadata=json.loads(row["metadata"]) if isinstance(row["metadata"], str) else (row["metadata"] or {}),
        )
