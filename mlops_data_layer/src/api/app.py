from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from ..config import settings
from ..storage.repository import MLOpsRepository
from ..features.registry import FeatureRegistry
from ..features.store import FeatureStore
from ..features.pipeline import FeatureEngineeringPipeline
from ..drift.detector import DriftDetector
from ..drift.monitor import DriftMonitor
from ..skew.detector import SkewDetector
from ..retraining.trigger import RetrainingTriggerEngine
from ..retraining.scheduler import RetrainingScheduler
from .routes import features, drift, retraining

log = structlog.get_logger(__name__)


def create_app(
    model_name: str = "default_model",
    model_version: str = "v1",
) -> FastAPI:

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # ── Bootstrap singletons ──────────────────────────────────────────
        redis_client = aioredis.from_url(settings.redis_url, decode_responses=False)
        repo = MLOpsRepository()
        await repo.init_db()

        registry = FeatureRegistry()
        store = FeatureStore(redis_client, registry)
        pipeline = FeatureEngineeringPipeline(model_name, registry, store)

        detector = DriftDetector(registry)
        drift_monitor = DriftMonitor(model_name, model_version, store, detector, redis_client)

        skew_detector = SkewDetector(registry)
        trigger_engine = RetrainingTriggerEngine(redis_client)
        scheduler = RetrainingScheduler(trigger_engine, redis_client)

        # ── Attach to app.state ───────────────────────────────────────────
        app.state.redis = redis_client
        app.state.repository = repo
        app.state.feature_registry = registry
        app.state.feature_store = store
        app.state.feature_pipeline = pipeline
        app.state.drift_monitor = drift_monitor
        app.state.skew_detector = skew_detector
        app.state.trigger_engine = trigger_engine
        app.state.retraining_scheduler = scheduler
        app.state.model_name = model_name
        app.state.model_version = model_version

        # ── Start background services ─────────────────────────────────────
        await drift_monitor.start()
        await scheduler.start()
        log.info("mlops_data_layer_started", model=model_name, version=model_version)

        yield

        # ── Shutdown ──────────────────────────────────────────────────────
        await drift_monitor.stop()
        await scheduler.stop()
        await repo.close()
        await redis_client.aclose()
        log.info("mlops_data_layer_stopped")

    app = FastAPI(
        title="MLOps Data Layer",
        description=(
            "Feature engineering pipelines, training/serving skew detection, "
            "drift monitoring, and automated retraining triggers."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Prometheus metrics scrape endpoint
    app.mount("/metrics", make_asgi_app())

    # Routes
    app.include_router(features.router)
    app.include_router(drift.router)
    app.include_router(retraining.router)

    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "mlops_data_layer"}

    @app.get("/api/v1/info")
    async def info(request):
        return {
            "model_name": request.app.state.model_name,
            "model_version": request.app.state.model_version,
            "features_registered": len(request.app.state.feature_registry),
            "drift_interval_seconds": settings.drift_eval_interval_seconds,
            "drift_window_size": settings.drift_eval_window_size,
        }

    return app
