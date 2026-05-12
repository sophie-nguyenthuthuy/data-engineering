from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.registry.core import SchemaRegistry
from .routes import router


def create_app(db_path: str = "registry.db") -> FastAPI:
    registry = SchemaRegistry(db_path=db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await registry.start()
        app.state.registry = registry
        yield
        await registry.stop()

    app = FastAPI(
        title="Schema Registry",
        description=(
            "Beyond Confluent: Schema Registry with Compatibility Enforcement, "
            "Auto-Migration, and Event Replay via Declarative DSL"
        ),
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router, prefix="/api/v1")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
