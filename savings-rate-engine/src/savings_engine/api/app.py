from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from savings_engine.storage.database import init_db
from .routes import banks, rates, analysis


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="Savings Rate Aggregation Engine",
        description=(
            "Real-time + historical Vietnamese bank savings rate data. "
            "Extensible backend for TiếtKiệm+."
        ),
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    app.include_router(banks.router,    prefix="/banks",    tags=["Banks"])
    app.include_router(rates.router,    prefix="/rates",    tags=["Rates"])
    app.include_router(analysis.router, prefix="/analysis", tags=["Analysis"])

    @app.get("/health", tags=["Meta"])
    def health():
        return {"status": "ok"}

    return app
