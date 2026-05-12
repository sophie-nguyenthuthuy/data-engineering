"""
Online store — Redis-backed, sub-10ms feature retrieval.

Key schema:  fs:{group}:{entity_id}
Value:       MessagePack-encoded dict of feature_name -> value
TTL:         Per-group from feature registry
"""
from __future__ import annotations

import struct
import time
from typing import Any

import msgpack
import redis
import redis.asyncio as aioredis
import structlog

log = structlog.get_logger(__name__)

# msgpack doesn't ship with feature_store, fallback to json bytes
try:
    import msgpack
    _USE_MSGPACK = True
except ImportError:
    import json as _json
    _USE_MSGPACK = False


def _encode(obj: dict) -> bytes:
    if _USE_MSGPACK:
        return msgpack.packb(obj, use_bin_type=True)
    return _json.dumps(obj).encode()


def _decode(raw: bytes) -> dict:
    if _USE_MSGPACK:
        return msgpack.unpackb(raw, raw=False)
    return _json.loads(raw)


def _make_key(group: str, entity_id: str) -> str:
    return f"fs:{group}:{entity_id}"


class OnlineStore:
    """Synchronous online store — use in ingestion pipeline and sync contexts."""

    def __init__(self, redis_url: str = "redis://localhost:6379", **kwargs: Any) -> None:
        self._client = redis.from_url(
            redis_url,
            decode_responses=False,
            socket_connect_timeout=0.5,
            socket_timeout=0.01,         # 10ms hard cap
            **kwargs,
        )
        self._pipeline_batch_size = 100

    # ------------------------------------------------------------------ #
    # Write                                                                #
    # ------------------------------------------------------------------ #

    def put(
        self,
        group: str,
        entity_id: str,
        features: dict[str, Any],
        ttl_seconds: int = 86400,
    ) -> None:
        key = _make_key(group, entity_id)
        payload = _encode({"v": features, "ts": int(time.time() * 1000)})
        self._client.set(key, payload, ex=ttl_seconds)

    def put_batch(
        self,
        group: str,
        records: list[tuple[str, dict[str, Any]]],
        ttl_seconds: int = 86400,
    ) -> None:
        """Pipeline batch write — one round-trip for up to pipeline_batch_size keys."""
        for chunk_start in range(0, len(records), self._pipeline_batch_size):
            chunk = records[chunk_start : chunk_start + self._pipeline_batch_size]
            pipe = self._client.pipeline(transaction=False)
            ts_ms = int(time.time() * 1000)
            for entity_id, features in chunk:
                key = _make_key(group, entity_id)
                payload = _encode({"v": features, "ts": ts_ms})
                pipe.set(key, payload, ex=ttl_seconds)
            pipe.execute()

    # ------------------------------------------------------------------ #
    # Read                                                                 #
    # ------------------------------------------------------------------ #

    def get(self, group: str, entity_id: str) -> dict[str, Any] | None:
        raw = self._client.get(_make_key(group, entity_id))
        if raw is None:
            return None
        record = _decode(raw)
        return record["v"]

    def get_batch(
        self, group: str, entity_ids: list[str]
    ) -> dict[str, dict[str, Any] | None]:
        """Single MGET round-trip — critical for <10ms multi-entity retrieval."""
        keys = [_make_key(group, eid) for eid in entity_ids]
        raws = self._client.mget(keys)
        result: dict[str, dict | None] = {}
        for entity_id, raw in zip(entity_ids, raws):
            result[entity_id] = _decode(raw)["v"] if raw else None
        return result

    def get_multi_group(
        self, requests: list[tuple[str, str]]
    ) -> dict[tuple[str, str], dict[str, Any] | None]:
        """
        Fetch features across multiple groups in one pipeline round-trip.
        requests: [(group, entity_id), ...]
        Returns: {(group, entity_id): features | None}
        """
        keys = [_make_key(g, e) for g, e in requests]
        raws = self._client.mget(keys)
        return {
            req: (_decode(raw)["v"] if raw else None)
            for req, raw in zip(requests, raws)
        }

    # ------------------------------------------------------------------ #
    # Delete / TTL management                                              #
    # ------------------------------------------------------------------ #

    def delete(self, group: str, entity_id: str) -> bool:
        return bool(self._client.delete(_make_key(group, entity_id)))

    def get_ttl(self, group: str, entity_id: str) -> int:
        return self._client.ttl(_make_key(group, entity_id))

    def healthcheck(self) -> bool:
        try:
            return self._client.ping()
        except Exception:
            return False


class AsyncOnlineStore:
    """Async online store — use in FastAPI serving path for maximum throughput."""

    def __init__(self, redis_url: str = "redis://localhost:6379", **kwargs: Any) -> None:
        self._client = aioredis.from_url(
            redis_url,
            decode_responses=False,
            socket_connect_timeout=0.5,
            socket_timeout=0.01,
            **kwargs,
        )

    async def get(self, group: str, entity_id: str) -> dict[str, Any] | None:
        raw = await self._client.get(_make_key(group, entity_id))
        if raw is None:
            return None
        return _decode(raw)["v"]

    async def get_multi_group(
        self, requests: list[tuple[str, str]]
    ) -> dict[tuple[str, str], dict[str, Any] | None]:
        """Async pipeline MGET — zero extra latency for multi-group fetches."""
        async with self._client.pipeline(transaction=False) as pipe:
            for g, e in requests:
                pipe.get(_make_key(g, e))
            raws = await pipe.execute()
        return {
            req: (_decode(raw)["v"] if raw else None)
            for req, raw in zip(requests, raws)
        }

    async def put(
        self,
        group: str,
        entity_id: str,
        features: dict[str, Any],
        ttl_seconds: int = 86400,
    ) -> None:
        key = _make_key(group, entity_id)
        payload = _encode({"v": features, "ts": int(time.time() * 1000)})
        await self._client.set(key, payload, ex=ttl_seconds)

    async def healthcheck(self) -> bool:
        try:
            return await self._client.ping()
        except Exception:
            return False

    async def close(self) -> None:
        await self._client.aclose()
