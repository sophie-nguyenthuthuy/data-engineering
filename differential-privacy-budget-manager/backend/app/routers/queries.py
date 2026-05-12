from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from ..database import get_db
from .. import models, schemas
from ..budget_manager import execute_query, BudgetNotFoundError, BudgetExhaustedError

router = APIRouter(prefix="/queries", tags=["queries"])


@router.post("/", response_model=schemas.QueryResponse)
def submit_query(payload: schemas.QueryRequest, db: Session = Depends(get_db)):
    try:
        return execute_query(db, payload)
    except BudgetNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs", response_model=List[schemas.QueryLogRead])
def get_query_logs(
    dataset_id: str = None,
    analyst_id: str = None,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    q = db.query(models.QueryLog).order_by(models.QueryLog.created_at.desc())
    if dataset_id:
        q = q.filter_by(dataset_id=dataset_id)
    if analyst_id:
        q = q.filter_by(analyst_id=analyst_id)
    return q.limit(limit).all()
