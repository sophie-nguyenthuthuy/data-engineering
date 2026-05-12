from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from typing import Annotated

import redis.asyncio as aioredis
import structlog
from fastapi import FastAPI, HTTPException, Query, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from ..config import settings
from ..storage.repository import ValidationRepository
from ..blocking.job_controller import JobController
from ..metrics.collector import MetricsCollector
from .websocket import websocket_endpoint, redis_subscription_loop

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Shared singletons (injected at startup)
# ---------------------------------------------------------------------------
_redis: aioredis.Redis | None = None
_repo: ValidationRepository | None = None
_job_ctrl: JobController | None = None
_collector: MetricsCollector | None = None


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app(
    repository: ValidationRepository,
    redis_client: aioredis.Redis,
    job_controller: JobController,
    collector: MetricsCollector,
) -> FastAPI:
    global _redis, _repo, _job_ctrl, _collector
    _redis = redis_client
    _repo = repository
    _job_ctrl = job_controller
    _collector = collector

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Start the Redis → WebSocket fan-out background task
        task = asyncio.create_task(
            redis_subscription_loop(redis_client), name="ws_fan_out"
        )
        log.info("dashboard_started")
        yield
        task.cancel()
        log.info("dashboard_stopped")

    app = FastAPI(
        title="Data Quality Monitor",
        description="Real-time data quality metrics and job gate API",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Mount Prometheus metrics at /metrics
    app.mount("/metrics", make_asgi_app())

    # ------------------------------------------------------------------
    # WebSocket
    # ------------------------------------------------------------------

    @app.websocket("/ws/metrics")
    async def ws_metrics(ws: WebSocket):
        await websocket_endpoint(ws, redis_client)

    # ------------------------------------------------------------------
    # REST endpoints
    # ------------------------------------------------------------------

    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "data_quality_monitor"}

    @app.get("/api/v1/snapshot")
    async def get_snapshot():
        """Latest MetricSnapshot (same payload the WebSocket streams)."""
        raw = await redis_client.get("dq:snapshot:latest")
        if not raw:
            return {"message": "No snapshot available yet"}
        import json
        return json.loads(raw)

    @app.get("/api/v1/results")
    async def list_results(
        table: Annotated[str | None, Query(description="Filter by table")] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
    ):
        return await repository.get_recent_results(limit=limit, table_name=table)

    @app.get("/api/v1/results/{result_id}")
    async def get_result(result_id: str):
        raw = await redis_client.get(f"dq:result:{result_id}")
        if not raw:
            raise HTTPException(status_code=404, detail="Result not found")
        import json
        return json.loads(raw)

    @app.get("/api/v1/blocks")
    async def list_blocks():
        """All currently active downstream job blocks."""
        return await job_controller.list_active_blocks()

    @app.delete("/api/v1/blocks/{job_name}")
    async def unblock_job(job_name: str):
        """Manually lift a block after human review."""
        removed = await job_controller.force_unblock(job_name)
        if not removed:
            raise HTTPException(status_code=404, detail=f"No active block for {job_name!r}")
        return {"message": f"Block for {job_name!r} lifted"}

    @app.get("/api/v1/blocks/{job_name}/status")
    async def block_status(job_name: str):
        blocked = await job_controller.is_blocked(job_name)
        return {"job_name": job_name, "blocked": blocked}

    @app.get("/api/v1/summary")
    async def get_summary(hours: Annotated[int, Query(ge=1, le=72)] = 1):
        return await repository.get_failure_summary(hours=hours)

    @app.get("/api/v1/tables/{table_name}/pass-rate")
    async def table_pass_rate(table_name: str):
        rate = await repository.get_pass_rate_last_hour(table_name)
        return {"table_name": table_name, "pass_rate_last_hour": rate}

    return app
