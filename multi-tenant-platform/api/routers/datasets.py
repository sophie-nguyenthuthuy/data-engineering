from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from api.dependencies import CurrentTenant, get_db, require_role
from api.schemas.dataset import DatasetCreate, DatasetResponse, DatasetUpdate
from core.security.rls import set_tenant_context
from db.models.data import Dataset


router = APIRouter()


@router.get("/", response_model=list[DatasetResponse])
async def list_datasets(
    tenant: CurrentTenant,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> list[DatasetResponse]:
    async with set_tenant_context(db, tenant.tenant_id):
        result = await db.execute(
            select(Dataset).where(Dataset.tenant_id == tenant.tenant_id)
        )
        return [DatasetResponse.model_validate(row) for row in result.scalars()]


@router.post("/", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_dataset(
    body: DatasetCreate,
    tenant: Annotated[CurrentTenant, Depends(require_role("owner", "admin", "member"))],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> DatasetResponse:
    dataset = Dataset(
        id=uuid4(),
        tenant_id=tenant.tenant_id,
        name=body.name,
        description=body.description,
        schema_definition=body.schema_definition,
        is_public=body.is_public,
    )
    async with set_tenant_context(db, tenant.tenant_id):
        db.add(dataset)
        await db.flush()
        return DatasetResponse.model_validate(dataset)


@router.get("/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(
    dataset_id: UUID,
    tenant: CurrentTenant,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> DatasetResponse:
    async with set_tenant_context(db, tenant.tenant_id):
        result = await db.execute(
            select(Dataset).where(Dataset.id == dataset_id)
        )
        dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return DatasetResponse.model_validate(dataset)


@router.patch("/{dataset_id}", response_model=DatasetResponse)
async def update_dataset(
    dataset_id: UUID,
    body: DatasetUpdate,
    tenant: Annotated[CurrentTenant, Depends(require_role("owner", "admin", "member"))],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> DatasetResponse:
    updates = body.model_dump(exclude_none=True)
    async with set_tenant_context(db, tenant.tenant_id):
        await db.execute(
            update(Dataset)
            .where(Dataset.id == dataset_id, Dataset.tenant_id == tenant.tenant_id)
            .values(**updates)
        )
        result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
        dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return DatasetResponse.model_validate(dataset)


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset(
    dataset_id: UUID,
    tenant: Annotated[CurrentTenant, Depends(require_role("owner", "admin"))],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> None:
    async with set_tenant_context(db, tenant.tenant_id):
        result = await db.execute(
            select(Dataset).where(Dataset.id == dataset_id, Dataset.tenant_id == tenant.tenant_id)
        )
        dataset = result.scalar_one_or_none()
        if dataset is None:
            raise HTTPException(status_code=404, detail="Dataset not found")
        await db.delete(dataset)
