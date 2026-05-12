from __future__ import annotations
from typing import Annotated

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter(prefix="/api/v1/features", tags=["features"])

# Singletons injected at app startup via app.state
def _store(request):
    return request.app.state.feature_store

def _registry(request):
    return request.app.state.feature_registry

def _pipeline(request):
    return request.app.state.feature_pipeline

def _repo(request):
    return request.app.state.repository


class IngestRequest(BaseModel):
    model_name: str
    model_version: str
    entity_id_col: str = "entity_id"
    records: list[dict]


@router.post("/ingest/training")
async def ingest_training_data(body: IngestRequest, request):
    """Run the feature engineering pipeline in training mode and capture a snapshot."""
    from fastapi import Request
    pipeline = request.app.state.feature_pipeline
    repo = request.app.state.repository
    df = pd.DataFrame(body.records)
    _, snapshot, run = await pipeline.run_training(df, body.model_version, body.entity_id_col)
    await repo.save_pipeline_run(run)
    await repo.save_snapshot(snapshot)
    return {
        "run_id": run.run_id,
        "snapshot_id": snapshot.snapshot_id,
        "status": run.status,
        "rows": run.output_rows,
        "duration_ms": run.duration_ms,
    }


@router.post("/ingest/serving")
async def ingest_serving_data(body: IngestRequest, request):
    """Run the feature engineering pipeline in serving mode."""
    pipeline = request.app.state.feature_pipeline
    df = pd.DataFrame(body.records)
    _, run = await pipeline.run_serving(df, body.entity_id_col)
    return {"run_id": run.run_id, "status": run.status, "rows": run.output_rows}


@router.get("/vector/{model_name}/{entity_id}")
async def get_feature_vector(model_name: str, entity_id: str, request):
    store = request.app.state.feature_store
    vec = await store.read_vector(entity_id, model_name)
    if not vec:
        raise HTTPException(status_code=404, detail="Vector not found")
    return vec


@router.get("/registry")
async def list_features(request):
    registry = request.app.state.feature_registry
    return [f.model_dump() for f in registry.all_features()]


@router.get("/snapshot/{model_name}/latest")
async def get_latest_snapshot(model_name: str, request):
    repo = request.app.state.repository
    snap = await repo.get_latest_snapshot(model_name)
    if not snap:
        raise HTTPException(status_code=404, detail="No snapshot found")
    return snap
