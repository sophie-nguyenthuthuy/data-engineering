from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from ..database import get_db
from .. import models, schemas

router = APIRouter(prefix="/budgets", tags=["budgets"])


@router.post("/", response_model=schemas.BudgetAllocationRead)
def create_allocation(payload: schemas.BudgetAllocationCreate, db: Session = Depends(get_db)):
    # Verify dataset and analyst exist
    if not db.query(models.Dataset).filter_by(id=payload.dataset_id).first():
        raise HTTPException(status_code=404, detail="Dataset not found")
    if not db.query(models.Analyst).filter_by(id=payload.analyst_id).first():
        raise HTTPException(status_code=404, detail="Analyst not found")

    existing = (
        db.query(models.BudgetAllocation)
        .filter_by(dataset_id=payload.dataset_id, analyst_id=payload.analyst_id)
        .first()
    )
    if existing:
        raise HTTPException(status_code=400, detail="Budget allocation already exists for this analyst/dataset pair")

    alloc = models.BudgetAllocation(**payload.model_dump())
    db.add(alloc)
    db.commit()
    db.refresh(alloc)
    return alloc


@router.get("/", response_model=List[schemas.BudgetAllocationRead])
def list_allocations(
    dataset_id: str = None,
    analyst_id: str = None,
    db: Session = Depends(get_db),
):
    q = db.query(models.BudgetAllocation)
    if dataset_id:
        q = q.filter_by(dataset_id=dataset_id)
    if analyst_id:
        q = q.filter_by(analyst_id=analyst_id)
    return q.all()


@router.get("/{allocation_id}", response_model=schemas.BudgetAllocationRead)
def get_allocation(allocation_id: str, db: Session = Depends(get_db)):
    alloc = db.query(models.BudgetAllocation).filter_by(id=allocation_id).first()
    if not alloc:
        raise HTTPException(status_code=404, detail="Allocation not found")
    return alloc


@router.patch("/{allocation_id}", response_model=schemas.BudgetAllocationRead)
def update_allocation(
    allocation_id: str,
    payload: schemas.BudgetAllocationUpdate,
    db: Session = Depends(get_db),
):
    alloc = db.query(models.BudgetAllocation).filter_by(id=allocation_id).first()
    if not alloc:
        raise HTTPException(status_code=404, detail="Allocation not found")
    for field, value in payload.model_dump(exclude_none=True).items():
        setattr(alloc, field, value)
    db.commit()
    db.refresh(alloc)
    return alloc


@router.post("/{allocation_id}/reset")
def reset_budget(allocation_id: str, db: Session = Depends(get_db)):
    alloc = db.query(models.BudgetAllocation).filter_by(id=allocation_id).first()
    if not alloc:
        raise HTTPException(status_code=404, detail="Allocation not found")
    alloc.consumed_epsilon = 0.0
    alloc.consumed_delta = 0.0
    db.commit()
    return {"detail": "Budget reset", "allocation_id": allocation_id}


@router.get("/summary/all", response_model=List[schemas.BudgetSummary])
def budget_summary(db: Session = Depends(get_db)):
    allocs = db.query(models.BudgetAllocation).all()
    result = []
    for a in allocs:
        query_count = db.query(models.QueryLog).filter_by(
            dataset_id=a.dataset_id, analyst_id=a.analyst_id
        ).count()
        result.append(schemas.BudgetSummary(
            dataset_id=a.dataset_id,
            dataset_name=a.dataset.name,
            analyst_id=a.analyst_id,
            analyst_username=a.analyst.username,
            total_epsilon=a.total_epsilon,
            consumed_epsilon=a.consumed_epsilon,
            remaining_epsilon=a.remaining_epsilon,
            percent_used=round(100 * a.consumed_epsilon / a.total_epsilon, 2) if a.total_epsilon else 0,
            is_exhausted=a.is_exhausted,
            query_count=query_count,
            exhaustion_policy=a.exhaustion_policy,
        ))
    return result
