"""
Entry point — spins up FastAPI, the regional store, and the replication engine.
"""
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from fastapi.responses import HTMLResponse
import os

from src.config import settings
from src.store.database import RegionalStore
from src.replication.engine import ReplicationEngine
from src.api.routes import router as data_router
from src.api.health import router as health_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("mesh.main")

# Jinja2 templates for the dashboard
_templates_dir = os.path.join(os.path.dirname(__file__), "dashboard", "templates")
templates = Jinja2Templates(directory=_templates_dir)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────
    store = RegionalStore(settings.db_path)
    await store.open()
    app.state.store = store

    engine = ReplicationEngine(
        store=store,
        region_id=settings.region_id,
        conflict_strategy=settings.conflict_strategy,
    )
    await engine.start()
    app.state.engine = engine

    log.info("Node started: region=%s port=%d strategy=%s",
             settings.region_id, settings.port, settings.conflict_strategy)
    yield

    # ── Shutdown ─────────────────────────────────────────────────────
    await engine.stop()
    await store.close()
    log.info("Node shut down cleanly.")


app = FastAPI(
    title=f"Data Mesh Node — {settings.region_id}",
    description=(
        "Active-active multi-region data product node with "
        "vector-clock conflict resolution and live replication metrics."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(data_router)
app.include_router(health_router)


@app.get("/dashboard", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard(request: Request):
    """Live health & replication dashboard."""
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {"region_id": settings.region_id},
    )


@app.get("/", tags=["Root"])
async def root():
    return {
        "region": settings.region_id,
        "docs": "/docs",
        "dashboard": "/dashboard",
        "health": "/health",
    }
