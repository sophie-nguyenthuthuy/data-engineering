"""
FastAPI server with REST endpoints and WebSocket push for real-time updates.

Endpoints
---------
GET  /api/health           — liveness
GET  /api/graph            — current nodes + edges snapshot
GET  /api/metrics          — latest risk metrics
GET  /api/alerts           — recent alert log
GET  /api/institutions/{id} — single institution detail
POST /api/simulate/{id}    — run contagion simulation from node
WS   /ws                   — streaming updates (graph + alerts)
"""

import asyncio
import json
import logging
import time
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from src.config import settings

log = logging.getLogger(__name__)

app = FastAPI(
    title="Systemic Risk Monitor",
    description="Real-time interbank contagion risk detection",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Injected by main.py at startup
_state: dict[str, Any] = {}


class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        log.info("WS client connected (%d total)", len(self.active))

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)
        log.info("WS client disconnected (%d total)", len(self.active))

    async def broadcast(self, message: dict):
        data = json.dumps(message)
        dead = []
        for ws in self.active:
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.active.remove(ws)


manager = ConnectionManager()


async def push_update(payload: dict):
    """Called by the monitor loop to broadcast state to WS clients."""
    await manager.broadcast(payload)


# ------------------------------------------------------------------ #
# REST endpoints
# ------------------------------------------------------------------ #


@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": time.time()}


@app.get("/api/graph")
async def get_graph():
    mg = _state.get("memgraph")
    if not mg:
        raise HTTPException(503, "Graph DB not ready")
    nodes = await mg.get_all_nodes()
    edges = await mg.get_all_edges()
    return {"nodes": nodes, "edges": edges}


@app.get("/api/metrics")
async def get_metrics():
    return _state.get("latest_metrics", {})


@app.get("/api/alerts")
async def get_alerts(limit: int = 50):
    engine = _state.get("alert_engine")
    if not engine:
        return {"alerts": []}
    return {"alerts": engine.recent(limit)}


@app.get("/api/institutions/{inst_id}")
async def get_institution(inst_id: str):
    mg = _state.get("memgraph")
    registry = _state.get("registry")
    if not mg or not registry:
        raise HTTPException(503, "Not ready")
    if inst_id not in registry.institutions:
        raise HTTPException(404, f"Institution {inst_id} not found")
    exposures = await mg.get_node_exposures(inst_id)
    inst = registry.get(inst_id)
    return {
        "id": inst.id,
        "name": inst.name,
        "tier": inst.tier,
        "balance": inst.balance,
        "lending_capacity": inst.lending_capacity,
        **exposures,
    }


@app.post("/api/simulate/{inst_id}")
async def simulate_contagion(inst_id: str, shock_pct: float = 0.30):
    from src.algorithms.contagion import simulate_cascade
    mg = _state.get("memgraph")
    registry = _state.get("registry")
    if not mg or not registry:
        raise HTTPException(503, "Not ready")
    if inst_id not in registry.institutions:
        raise HTTPException(404, f"Institution {inst_id} not found")
    edges = await mg.get_all_edges()
    result = simulate_cascade(edges, inst_id, shock_fraction=shock_pct)
    return {
        "seed": result.seed_node,
        "failed_nodes": result.failed_nodes,
        "cascade_depth": result.cascade_depth,
        "fraction_failed": result.fraction_failed,
        "total_exposure_lost": result.total_exposure_lost,
    }


# ------------------------------------------------------------------ #
# WebSocket
# ------------------------------------------------------------------ #


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    # Send current state immediately on connect
    current = _state.get("latest_metrics")
    if current:
        await ws.send_text(json.dumps({"type": "snapshot", "data": current}))
    try:
        while True:
            await ws.receive_text()  # keep-alive / handle client pings
    except WebSocketDisconnect:
        manager.disconnect(ws)


# ------------------------------------------------------------------ #
# Dashboard HTML
# ------------------------------------------------------------------ #


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    import pathlib
    html_path = pathlib.Path(__file__).parent.parent.parent / "dashboard" / "index.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text())
    return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)


def start(state: dict):
    global _state
    _state = state
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
