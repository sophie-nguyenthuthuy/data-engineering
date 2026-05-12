"""
Online feature store backed by Redis.
Stores the latest feature vector per entity (user_id) and a
bounded ring-buffer of recent feature values for drift monitoring.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

import redis


_RING_BUFFER_SIZE = 10_000  # max recent values kept per feature


class OnlineStore:
    def __init__(self, url: str | None = None) -> None:
        url = url or os.getenv("REDIS_URL", "redis://localhost:6379")
        self._client = redis.from_url(url, decode_responses=True)

    # ------------------------------------------------------------------
    # Entity feature vectors
    # ------------------------------------------------------------------

    def set_features(self, entity_id: str, features: dict[str, Any], ttl_seconds: int = 86400) -> None:
        key = f"fv:{entity_id}"
        self._client.hset(key, mapping={k: json.dumps(v) for k, v in features.items()})
        self._client.expire(key, ttl_seconds)

    def get_features(self, entity_id: str, feature_names: list[str] | None = None) -> dict[str, Any]:
        key = f"fv:{entity_id}"
        if feature_names:
            raw = self._client.hmget(key, feature_names)
            return {n: json.loads(v) for n, v in zip(feature_names, raw) if v is not None}
        raw = self._client.hgetall(key)
        return {k: json.loads(v) for k, v in raw.items()}

    # ------------------------------------------------------------------
    # Production distribution ring-buffer (for drift detection)
    # ------------------------------------------------------------------

    def push_feature_value(self, feature_name: str, value: Any) -> None:
        """Append a production feature value; trim to ring-buffer size."""
        key = f"dist:{feature_name}"
        pipe = self._client.pipeline()
        pipe.rpush(key, json.dumps(value))
        pipe.ltrim(key, -_RING_BUFFER_SIZE, -1)
        pipe.execute()

    def get_recent_values(self, feature_name: str, n: int = _RING_BUFFER_SIZE) -> list[Any]:
        key = f"dist:{feature_name}"
        raw = self._client.lrange(key, -n, -1)
        return [json.loads(v) for v in raw]

    def clear_distribution(self, feature_name: str) -> None:
        self._client.delete(f"dist:{feature_name}")

    # ------------------------------------------------------------------
    # Global stats (mean/stddev for z-score, etc.)
    # ------------------------------------------------------------------

    def set_global_stats(self, stats: dict[str, float]) -> None:
        self._client.hset("global_stats", mapping={k: str(v) for k, v in stats.items()})

    def get_global_stats(self) -> dict[str, float]:
        raw = self._client.hgetall("global_stats")
        return {k: float(v) for k, v in raw.items()}

    # ------------------------------------------------------------------
    # Drift report persistence
    # ------------------------------------------------------------------

    def set_drift_report(self, report: dict) -> None:
        self._client.set("drift:latest", json.dumps(report))
        self._client.rpush("drift:history", json.dumps({**report, "saved_at": time.time()}))
        self._client.ltrim("drift:history", -90, -1)

    def get_drift_report(self) -> dict | None:
        raw = self._client.get("drift:latest")
        return json.loads(raw) if raw else None

    def get_drift_history(self) -> list[dict]:
        raw = self._client.lrange("drift:history", 0, -1)
        return [json.loads(r) for r in raw]

    def ping(self) -> bool:
        try:
            return self._client.ping()  # type: ignore[return-value]
        except redis.RedisError:
            return False
