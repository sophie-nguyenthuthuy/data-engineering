from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status

from src.registry.core import SchemaRegistry
from src.registry.models import (
    CompatibilityMode,
    MigrationScript,
    MigrationStep,
    SchemaType,
    TransformEvent,
)
from src.migration.dsl import DSLParseError, TransformationDSL
from src.migration.executor import MigrationExecutor
from src.migration.generator import MigrationGenerator
from src.replay.engine import ReplayEngine

from .schemas import (
    CheckCompatibilityRequest,
    DSLMigrationRequest,
    MigratePayloadRequest,
    RegisterSchemaRequest,
    ReplayRequest,
    SetConfigRequest,
)

router = APIRouter()


def get_registry(request: Request) -> SchemaRegistry:
    return request.app.state.registry


# ── Subjects ──────────────────────────────────────────────────────────────────

@router.get("/subjects", tags=["subjects"])
async def list_subjects(registry: SchemaRegistry = Depends(get_registry)) -> list[str]:
    return await registry.list_subjects()


@router.delete("/subjects/{subject}", tags=["subjects"])
async def delete_subject(subject: str, registry: SchemaRegistry = Depends(get_registry)) -> dict:
    count = await registry.delete_subject(subject)
    return {"subject": subject, "versions_deleted": count}


# ── Config ────────────────────────────────────────────────────────────────────

@router.get("/config/{subject}", tags=["config"])
async def get_config(subject: str, registry: SchemaRegistry = Depends(get_registry)):
    return await registry.get_config(subject)


@router.put("/config/{subject}", tags=["config"])
async def set_config(
    subject: str,
    body: SetConfigRequest,
    registry: SchemaRegistry = Depends(get_registry),
):
    return await registry.set_config(subject, body.compatibility)


# ── Schema Versions ───────────────────────────────────────────────────────────

@router.get("/subjects/{subject}/versions", tags=["schemas"])
async def list_versions(subject: str, registry: SchemaRegistry = Depends(get_registry)):
    return await registry.list_versions(subject)


@router.get("/subjects/{subject}/versions/{version}", tags=["schemas"])
async def get_schema(
    subject: str, version: str, registry: SchemaRegistry = Depends(get_registry)
):
    sv = await registry.get_schema(subject, version)
    if sv is None:
        raise HTTPException(status_code=404, detail=f"Schema v{version} not found")
    return sv


@router.post("/subjects/{subject}/versions", tags=["schemas"], status_code=201)
async def register_schema(
    subject: str,
    body: RegisterSchemaRequest,
    registry: SchemaRegistry = Depends(get_registry),
):
    try:
        sv = await registry.register_schema(
            subject=subject,
            schema_definition=body.schema_definition,
            schema_type=body.schema_type,
            metadata=body.metadata,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return sv


@router.delete("/subjects/{subject}/versions/{version}", tags=["schemas"])
async def delete_version(
    subject: str, version: int, registry: SchemaRegistry = Depends(get_registry)
):
    ok = await registry.delete_version(subject, version)
    if not ok:
        raise HTTPException(status_code=404, detail="Version not found")
    return {"deleted": version}


# ── Compatibility ─────────────────────────────────────────────────────────────

@router.post("/compatibility/subjects/{subject}/versions", tags=["compatibility"])
async def check_compatibility(
    subject: str,
    body: CheckCompatibilityRequest,
    registry: SchemaRegistry = Depends(get_registry),
):
    result = await registry.check_compatibility(subject, body.schema_definition, body.mode)
    return result


# ── Migrations ────────────────────────────────────────────────────────────────

@router.get("/subjects/{subject}/migrations", tags=["migrations"])
async def list_migrations(subject: str, registry: SchemaRegistry = Depends(get_registry)):
    return await registry.list_migrations(subject)


@router.get("/subjects/{subject}/migrations/{from_version}/{to_version}", tags=["migrations"])
async def get_migration(
    subject: str,
    from_version: int,
    to_version: int,
    registry: SchemaRegistry = Depends(get_registry),
):
    m = await registry.get_migration(subject, from_version, to_version)
    if m is None:
        raise HTTPException(status_code=404, detail="Migration not found")
    return m


@router.post("/subjects/{subject}/migrations/generate/{from_version}/{to_version}", tags=["migrations"])
async def generate_migration(
    subject: str,
    from_version: int,
    to_version: int,
    registry: SchemaRegistry = Depends(get_registry),
):
    sv_from = await registry.get_schema(subject, from_version)
    sv_to = await registry.get_schema(subject, to_version)
    if sv_from is None or sv_to is None:
        raise HTTPException(status_code=404, detail="One or both schema versions not found")
    gen = MigrationGenerator()
    script = gen.generate(subject, from_version, to_version, sv_from.schema_definition, sv_to.schema_definition)
    saved = await registry.save_migration(script)
    return saved


@router.put("/subjects/{subject}/migrations/{from_version}/{to_version}/dsl", tags=["migrations"])
async def upload_dsl_migration(
    subject: str,
    from_version: int,
    to_version: int,
    body: DSLMigrationRequest,
    registry: SchemaRegistry = Depends(get_registry),
):
    dsl = TransformationDSL()
    try:
        steps = dsl.parse(body.dsl_source)
    except DSLParseError as e:
        raise HTTPException(status_code=422, detail=str(e))
    script = MigrationScript(
        subject=subject,
        from_version=from_version,
        to_version=to_version,
        steps=steps,
        dsl_source=body.dsl_source,
        auto_generated=False,
    )
    saved = await registry.save_migration(script)
    return saved


@router.post("/subjects/{subject}/migrate", tags=["migrations"])
async def migrate_payload(
    subject: str,
    body: MigratePayloadRequest,
    registry: SchemaRegistry = Depends(get_registry),
):
    chain = await registry.build_migration_chain(subject, body.from_version, body.to_version)
    if not chain:
        raise HTTPException(
            status_code=404,
            detail=f"No migration path from v{body.from_version} to v{body.to_version}",
        )
    executor = MigrationExecutor()
    result = executor.apply_chain(body.payload, chain)
    return {"migrated": result, "steps_applied": len(chain)}


# ── Replay ────────────────────────────────────────────────────────────────────

@router.post("/subjects/{subject}/replay", tags=["replay"])
async def replay_events(
    subject: str,
    body: ReplayRequest,
    registry: SchemaRegistry = Depends(get_registry),
):
    events = [
        TransformEvent(
            event_id=e.get("event_id", str(i)),
            subject=subject,
            schema_version=e["schema_version"],
            payload=e["payload"],
        )
        for i, e in enumerate(body.events)
    ]
    engine = ReplayEngine(registry)
    try:
        result = await engine.replay(subject, events, body.target_version, body.validate)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return result
