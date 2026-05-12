"""
Feature serving API (FastAPI).

Endpoints:
  GET  /features/{entity_id}          — latest feature vector for an entity
  POST /features/compute              — compute features on-the-fly from a raw event
  GET  /drift/latest                  — most recent drift report
  GET  /drift/history                 — all stored drift reports
  GET  /health                        — liveness check
  GET  /registry                      — list all registered features
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from feature_store.online_store import OnlineStore
from feature_store.transformations import build_registry

logger = logging.getLogger(__name__)
app = FastAPI(
    title="Streaming Feature Store",
    description="Online feature serving with training-serving skew detection",
    version="1.0.0",
)

_registry = build_registry()
_online_store = OnlineStore()


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class RawEvent(BaseModel):
    user_id: str
    amount: float = 0.0
    category: str = "unknown"
    timestamp: str | None = None
    user_age: int = 30
    account_created_at: str | None = None

    model_config = {"extra": "allow"}


class FeatureVector(BaseModel):
    entity_id: str
    features: dict[str, Any]
    source: str  # "online_store" | "computed"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    return {"status": "ok", "redis": _online_store.ping()}


@app.get("/registry")
def list_features() -> dict:
    return {
        "features": [
            {
                "name": f.name,
                "type": f.feature_type.value,
                "description": f.description,
                "tags": f.tags,
            }
            for f in _registry.all_features()
        ]
    }


@app.get("/features/{entity_id}", response_model=FeatureVector)
def get_features(entity_id: str) -> FeatureVector:
    features = _online_store.get_features(entity_id)
    if not features:
        raise HTTPException(status_code=404, detail=f"No features found for entity '{entity_id}'")
    return FeatureVector(entity_id=entity_id, features=features, source="online_store")


@app.post("/features/compute", response_model=FeatureVector)
def compute_features(event: RawEvent) -> FeatureVector:
    """Compute features on-the-fly without requiring prior streaming ingestion."""
    record = event.model_dump()
    context = _online_store.get_global_stats()

    features: dict[str, Any] = {}
    for feat in _registry.all_features():
        features[feat.name] = feat.compute(record, context)

    return FeatureVector(entity_id=event.user_id, features=features, source="computed")


@app.get("/drift/latest")
def get_latest_drift() -> dict:
    report = _online_store.get_drift_report()
    if report is None:
        raise HTTPException(status_code=404, detail="No drift report available yet")
    return report


@app.get("/drift/history")
def get_drift_history() -> dict:
    return {"reports": _online_store.get_drift_history()}
