from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from api.middleware.tenant import TenantMiddleware
from api.middleware.quota import QuotaMiddleware
from api.middleware.audit import AuditMiddleware
from api.routers import tenants, datasets, records, admin, auth
from core.config import settings
from db.session import get_engine


log = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    log.info("platform.starting", env=settings.environment)
    yield
    engine = get_engine()
    await engine.dispose()
    log.info("platform.stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Multi-Tenant Data Platform",
        version="0.1.0",
        docs_url="/docs" if settings.environment != "production" else None,
        lifespan=lifespan,
    )

    # Middleware — applied in reverse order (last added = outermost)
    app.add_middleware(AuditMiddleware)
    app.add_middleware(QuotaMiddleware)
    app.add_middleware(TenantMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.environment == "development" else [],
        allow_methods=["*"],
        allow_headers=["Authorization", "Content-Type", "X-Tenant-ID"],
    )

    # Routes
    app.include_router(auth.router, prefix="/auth", tags=["auth"])
    app.include_router(tenants.router, prefix="/tenants", tags=["tenants"])
    app.include_router(datasets.router, prefix="/datasets", tags=["datasets"])
    app.include_router(records.router, prefix="/datasets/{dataset_id}/records", tags=["records"])
    app.include_router(admin.router, prefix="/admin", tags=["admin"])

    # Prometheus metrics endpoint
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    return app


app = create_app()
