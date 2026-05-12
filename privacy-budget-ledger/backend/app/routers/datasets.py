from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from ..database import get_db
from .. import budget_manager, schemas

router = APIRouter(prefix="/datasets", tags=["datasets"])


@router.post("/", response_model=schemas.DatasetRead, status_code=201)
def create_dataset(data: schemas.DatasetCreate, db: Session = Depends(get_db)):
    return budget_manager.create_dataset(db, data)


@router.get("/", response_model=List[schemas.DatasetRead])
def list_datasets(db: Session = Depends(get_db)):
    return budget_manager.list_datasets(db)


@router.get("/{dataset_id}", response_model=schemas.DatasetRead)
def get_dataset(dataset_id: str, db: Session = Depends(get_db)):
    ds = budget_manager.get_dataset(db, dataset_id)
    if not ds:
        raise HTTPException(404, f"Dataset {dataset_id!r} not found")
    return ds


@router.delete("/{dataset_id}", status_code=204)
def delete_dataset(dataset_id: str, db: Session = Depends(get_db)):
    if not budget_manager.delete_dataset(db, dataset_id):
        raise HTTPException(404, f"Dataset {dataset_id!r} not found")
