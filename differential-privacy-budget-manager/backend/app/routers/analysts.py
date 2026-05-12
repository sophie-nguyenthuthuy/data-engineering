from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from ..database import get_db
from .. import models, schemas

router = APIRouter(prefix="/analysts", tags=["analysts"])


@router.post("/", response_model=schemas.AnalystRead)
def create_analyst(payload: schemas.AnalystCreate, db: Session = Depends(get_db)):
    if db.query(models.Analyst).filter_by(username=payload.username).first():
        raise HTTPException(status_code=400, detail="Username already exists")
    analyst = models.Analyst(**payload.model_dump())
    db.add(analyst)
    db.commit()
    db.refresh(analyst)
    return analyst


@router.get("/", response_model=List[schemas.AnalystRead])
def list_analysts(db: Session = Depends(get_db)):
    return db.query(models.Analyst).all()


@router.get("/{analyst_id}", response_model=schemas.AnalystRead)
def get_analyst(analyst_id: str, db: Session = Depends(get_db)):
    a = db.query(models.Analyst).filter_by(id=analyst_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Analyst not found")
    return a


@router.delete("/{analyst_id}")
def delete_analyst(analyst_id: str, db: Session = Depends(get_db)):
    a = db.query(models.Analyst).filter_by(id=analyst_id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Analyst not found")
    db.delete(a)
    db.commit()
    return {"detail": "deleted"}
