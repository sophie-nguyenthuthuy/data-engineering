from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from ..database import get_db
from .. import models, schemas

router = APIRouter(prefix="/datasets", tags=["datasets"])


@router.post("/", response_model=schemas.DatasetRead)
def create_dataset(payload: schemas.DatasetCreate, db: Session = Depends(get_db)):
    if db.query(models.Dataset).filter_by(name=payload.name).first():
        raise HTTPException(status_code=400, detail="Dataset name already exists")
    ds = models.Dataset(**payload.model_dump())
    db.add(ds)
    db.commit()
    db.refresh(ds)
    return ds


@router.get("/", response_model=List[schemas.DatasetRead])
def list_datasets(db: Session = Depends(get_db)):
    return db.query(models.Dataset).all()


@router.get("/{dataset_id}", response_model=schemas.DatasetRead)
def get_dataset(dataset_id: str, db: Session = Depends(get_db)):
    ds = db.query(models.Dataset).filter_by(id=dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return ds


@router.delete("/{dataset_id}")
def delete_dataset(dataset_id: str, db: Session = Depends(get_db)):
    ds = db.query(models.Dataset).filter_by(id=dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    db.delete(ds)
    db.commit()
    return {"detail": "deleted"}
