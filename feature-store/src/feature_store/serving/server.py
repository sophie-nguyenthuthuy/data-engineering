"""
Feature serving API — FastAPI server with <10ms p99 SLO.

Endpoints:
  GET  /features/{group}/{entity_id}         — single entity
  POST /features/batch                        — multi-entity, multi-group
  GET  /health
  GET  /metrics                               — Prometheus exposition
  GET  /registry                              — feature schema catalog
"""
from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from typing import Any

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.gzip import GZipMiddleware
from prometheus_client import Counter, Histogram, generate_latest
from pydantic import BaseModel

from feature_store.online.redis_store import AsyncOnlineStore
from feature_store.offline.parquet_store import OfflineStore
from feature_store.registry.feature_registry import FeatureRegistry

log = structlog.get_logger(__name__)

# ------------------------------------------------------------------ #
# Metrics                                                             #
# ------------------------------------------------------------------ #

REQUEST_LATENCY = Histogram(
    "fs_request_duration_seconds",
    "Feature fetch latency",
    ["endpoint"],
    buckets=[0.001, 0.003, 0.005, 0.008, 0.010, 0.025, 0.050, 0.100],
)
CACHE_HITS = Counter("fs_cache_hits_total", "L1 cache hits")
CACHE_MISSES = Counter("fs_cache_misses_total", "L1 cache misses")
STORE_ERRORS = Counter("fs_store_errors_total", "Store read errors", ["store"])

# ------------------------------------------------------------------ #
# L1 in-process cache (tiny, short TTL — takes edge off hot keys)    #
# ------------------------------------------------------------------ #

_L1_CACHE: dict[str, tuple[float, Any]] = {}
_L1_TTL = float(os.getenv("L1_CACHE_TTL_SECONDS", "1"))


def _l1_get(key: str) -> Any | None:
    entry = _L1_CACHE.get(key)
    if entry and (time.monotonic() - entry[0]) < _L1_TTL:
        CACHE_HITS.inc()
        return entry[1]
    CACHE_MISSES.inc()
    return None


def _l1_set(key: str, value: Any) -> None:
    _L1_CACHE[key] = (time.monotonic(), value)
    # simple eviction: cap at 10k entries
    if len(_L1_CACHE) > 10_000:
        oldest = sorted(_L1_CACHE, key=lambda k: _L1_CACHE[k][0])
        for k in oldest[:1_000]:
            _L1_CACHE.pop(k, None)


# ------------------------------------------------------------------ #
# App lifecycle                                                        #
# ------------------------------------------------------------------ #

online_store: AsyncOnlineStore
registry: FeatureRegistry
offline_store: OfflineStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    global online_store, registry, offline_store
    online_store = AsyncOnlineStore(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379")
    )
    offline_store = OfflineStore(
        base_path=os.getenv("OFFLINE_STORE_PATH", "./data/offline")
    )
    registry = FeatureRegistry()
    config_path = os.getenv("CONFIG_PATH", "configs/feature_store.yaml")
    if os.path.exists(config_path):
        registry.register_from_config(config_path)
        log.info("registry loaded", groups=registry.list_groups())
    yield
    await online_store.close()


app = FastAPI(
    title="Feature Store",
    description="Real-time dual-layer feature store",
    version="0.1.0",
    lifespan=lifespan,
)
app.add_middleware(GZipMiddleware, minimum_size=500)

# ------------------------------------------------------------------ #
# Request / Response models                                            #
# ------------------------------------------------------------------ #


class BatchRequest(BaseModel):
    requests: list[dict]  # [{group, entity_id}, ...]


class FeatureWriteRequest(BaseModel):
    entity_id: str
    features: dict[str, Any]
    ttl_seconds: int = 86400


# ------------------------------------------------------------------ #
# Endpoints                                                            #
# ------------------------------------------------------------------ #


@app.get("/features/{group}/{entity_id}")
async def get_features(group: str, entity_id: str) -> dict:
    t0 = time.monotonic()
    cache_key = f"{group}:{entity_id}"

    cached = _l1_get(cache_key)
    if cached is not None:
        REQUEST_LATENCY.labels("get").observe(time.monotonic() - t0)
        return {"group": group, "entity_id": entity_id, "features": cached, "source": "l1"}

    try:
        features = await online_store.get(group, entity_id)
    except Exception as exc:
        STORE_ERRORS.labels("redis").inc()
        log.error("online store read error", error=str(exc))
        raise HTTPException(status_code=503, detail="Store unavailable")

    if features is None:
        REQUEST_LATENCY.labels("get").observe(time.monotonic() - t0)
        raise HTTPException(status_code=404, detail=f"No features for {group}/{entity_id}")

    _l1_set(cache_key, features)
    elapsed_ms = (time.monotonic() - t0) * 1000
    REQUEST_LATENCY.labels("get").observe(elapsed_ms / 1000)

    if elapsed_ms > 10:
        log.warning("SLO breach", latency_ms=round(elapsed_ms, 2), group=group)

    return {"group": group, "entity_id": entity_id, "features": features, "source": "redis"}


@app.post("/features/batch")
async def get_features_batch(req: BatchRequest) -> dict:
    """
    Fetch features for multiple (group, entity_id) pairs in one request.
    Uses a single Redis pipeline — stays well under 10ms for typical payloads.
    """
    t0 = time.monotonic()
    requests = [(r["group"], r["entity_id"]) for r in req.requests]

    # Check L1 cache
    hits: dict = {}
    misses: list[tuple[str, str]] = []
    for g, e in requests:
        v = _l1_get(f"{g}:{e}")
        if v is not None:
            hits[(g, e)] = v
        else:
            misses.append((g, e))

    # Batch-fetch misses from Redis
    if misses:
        try:
            fetched = await online_store.get_multi_group(misses)
        except Exception as exc:
            STORE_ERRORS.labels("redis").inc()
            raise HTTPException(status_code=503, detail="Store unavailable")
        for (g, e), features in fetched.items():
            if features:
                _l1_set(f"{g}:{e}", features)
            hits[(g, e)] = features

    REQUEST_LATENCY.labels("batch").observe(time.monotonic() - t0)

    return {
        "results": [
            {
                "group": g,
                "entity_id": e,
                "features": hits.get((g, e)),
            }
            for g, e in requests
        ],
        "latency_ms": round((time.monotonic() - t0) * 1000, 2),
    }


@app.post("/features/{group}/{entity_id}")
async def write_feature(group: str, entity_id: str, req: FeatureWriteRequest) -> dict:
    """Direct write endpoint — useful for testing and backfills."""
    from feature_store.online.redis_store import OnlineStore
    sync_store = OnlineStore(redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"))
    sync_store.put(group, entity_id, req.features, req.ttl_seconds)
    offline_store.write(group, entity_id, req.features)
    return {"status": "ok", "group": group, "entity_id": entity_id}


@app.get("/health")
async def health() -> dict:
    redis_ok = await online_store.healthcheck()
    return {
        "status": "ok" if redis_ok else "degraded",
        "redis": redis_ok,
        "registry_groups": registry.list_groups(),
    }


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type="text/plain; version=0.0.4")


@app.get("/registry")
async def get_registry() -> dict:
    import json
    return json.loads(registry.to_json())


@app.get("/offline/stats/{group}")
async def offline_stats(group: str) -> dict:
    return offline_store.get_stats(group)


# ------------------------------------------------------------------ #
# Entry point                                                          #
# ------------------------------------------------------------------ #


def main() -> None:
    uvicorn.run(
        "feature_store.serving.server:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        workers=int(os.getenv("WORKERS", "4")),
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        loop="uvloop",
        access_log=False,           # avoid per-request log overhead
    )


if __name__ == "__main__":
    main()
