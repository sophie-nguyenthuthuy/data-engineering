"""FastAPI read layer + static dashboard."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from .config import settings
from .db import init_db, recent_prices, row_count

app = FastAPI(title="bitcoinMonitor", version="0.1.0")
init_db()

DASHBOARD_DIR = Path(__file__).resolve().parent.parent / "dashboard"


@app.get("/health")
def health() -> dict:
    return {"ok": True, "rows": row_count(), "assets": list(settings.assets)}


@app.get("/assets")
def assets() -> dict:
    return {"assets": list(settings.assets), "vs_currency": settings.vs_currency}


@app.get("/prices")
def prices(
    asset: str = Query(default="bitcoin"),
    limit: int = Query(default=500, ge=1, le=10000),
) -> dict:
    if asset not in settings.assets:
        raise HTTPException(status_code=404, detail=f"asset '{asset}' not tracked")
    return {
        "asset": asset,
        "vs_currency": settings.vs_currency,
        "points": recent_prices(asset, settings.vs_currency, limit),
    }


@app.get("/")
def index():
    f = DASHBOARD_DIR / "index.html"
    if not f.exists():
        raise HTTPException(status_code=404, detail="dashboard not built")
    return FileResponse(f)
