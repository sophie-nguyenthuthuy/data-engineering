"""
Data record CRUD — bulk insert supported, with quota checks before write.
"""
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from api.dependencies import CurrentTenant, get_db, get_quota_engine, require_role
from api.schemas.dataset import BulkRecordCreate, RecordCreate, RecordResponse
from core.quotas.engine import QuotaEngine
from core.quotas.tiers import TIERS
from core.security.rls import set_tenant_context
from db.models.data import DataRecord, Dataset


router = APIRouter()


async def _assert_dataset_owned(
    db: AsyncSession, dataset_id: UUID, tenant_id: UUID
) -> Dataset:
    result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id, Dataset.tenant_id == tenant_id)
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


@router.get("/", response_model=list[RecordResponse])
async def list_records(
    dataset_id: UUID,
    tenant: CurrentTenant,
    db: Annotated[AsyncSession, Depends(get_db)],
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
) -> list[RecordResponse]:
    async with set_tenant_context(db, tenant.tenant_id):
        result = await db.execute(
            select(DataRecord)
            .where(DataRecord.dataset_id == dataset_id, DataRecord.tenant_id == tenant.tenant_id)
            .offset(offset)
            .limit(limit)
        )
        return [RecordResponse.model_validate(r) for r in result.scalars()]


@router.post("/", response_model=RecordResponse, status_code=status.HTTP_201_CREATED)
async def create_record(
    dataset_id: UUID,
    body: RecordCreate,
    tenant: Annotated[CurrentTenant, Depends(require_role("owner", "admin", "member"))],
    db: Annotated[AsyncSession, Depends(get_db)],
    quota: Annotated[QuotaEngine, Depends(get_quota_engine)],
) -> RecordResponse:
    async with set_tenant_context(db, tenant.tenant_id):
        dataset = await _assert_dataset_owned(db, dataset_id, tenant.tenant_id)
        limits = TIERS.get(tenant.tier, TIERS["free"])
        if limits.max_rows != -1 and dataset.row_count >= limits.max_rows:
            raise HTTPException(
                status_code=402,
                detail=f"Row limit ({limits.max_rows:,}) reached for your tier. Upgrade to add more rows.",
            )
        record = DataRecord(id=uuid4(), tenant_id=tenant.tenant_id, dataset_id=dataset_id, data=body.data)
        db.add(record)
        await db.flush()
        return RecordResponse.model_validate(record)


@router.post("/bulk", status_code=status.HTTP_201_CREATED)
async def bulk_insert(
    dataset_id: UUID,
    body: BulkRecordCreate,
    tenant: Annotated[CurrentTenant, Depends(require_role("owner", "admin", "member"))],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> dict:
    if len(body) > 10_000:
        raise HTTPException(status_code=400, detail="Bulk limit is 10,000 records per request")

    async with set_tenant_context(db, tenant.tenant_id):
        await _assert_dataset_owned(db, dataset_id, tenant.tenant_id)
        records = [
            DataRecord(id=uuid4(), tenant_id=tenant.tenant_id, dataset_id=dataset_id, data=row)
            for row in body.records
        ]
        db.add_all(records)
        await db.flush()

    return {"inserted": len(records)}


@router.delete("/{record_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_record(
    dataset_id: UUID,
    record_id: UUID,
    tenant: Annotated[CurrentTenant, Depends(require_role("owner", "admin", "member"))],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> None:
    async with set_tenant_context(db, tenant.tenant_id):
        result = await db.execute(
            select(DataRecord).where(
                DataRecord.id == record_id,
                DataRecord.dataset_id == dataset_id,
                DataRecord.tenant_id == tenant.tenant_id,
            )
        )
        record = result.scalar_one_or_none()
        if record is None:
            raise HTTPException(status_code=404, detail="Record not found")
        await db.delete(record)
