from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from ..database import get_db
from .. import budget_manager, schemas

router = APIRouter(prefix="/analysts", tags=["analysts"])


@router.post("/", response_model=schemas.AnalystRead, status_code=201)
def create_analyst(data: schemas.AnalystCreate, db: Session = Depends(get_db)):
    return budget_manager.create_analyst(db, data)


@router.get("/", response_model=List[schemas.AnalystRead])
def list_analysts(db: Session = Depends(get_db)):
    return budget_manager.list_analysts(db)


@router.get("/{analyst_id}", response_model=schemas.AnalystRead)
def get_analyst(analyst_id: str, db: Session = Depends(get_db)):
    a = budget_manager.get_analyst(db, analyst_id)
    if not a:
        raise HTTPException(404, f"Analyst {analyst_id!r} not found")
    return a


@router.delete("/{analyst_id}", status_code=204)
def delete_analyst(analyst_id: str, db: Session = Depends(get_db)):
    if not budget_manager.delete_analyst(db, analyst_id):
        raise HTTPException(404, f"Analyst {analyst_id!r} not found")
