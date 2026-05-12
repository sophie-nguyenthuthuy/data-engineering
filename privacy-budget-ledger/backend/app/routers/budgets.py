from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from ..database import get_db
from .. import budget_manager, schemas
from ..query_planner import get_composition_summary

router = APIRouter(prefix="/budgets", tags=["budgets"])


@router.post("/", response_model=schemas.BudgetAllocationRead, status_code=201)
def create_allocation(data: schemas.BudgetAllocationCreate, db: Session = Depends(get_db)):
    return budget_manager.create_allocation(db, data)


@router.get("/", response_model=List[schemas.BudgetAllocationRead])
def list_allocations(db: Session = Depends(get_db)):
    return budget_manager.list_allocations(db)


@router.get("/{dataset_id}/{analyst_id}", response_model=schemas.BudgetAllocationRead)
def get_allocation(dataset_id: str, analyst_id: str, db: Session = Depends(get_db)):
    alloc = budget_manager.get_allocation(db, dataset_id, analyst_id)
    if not alloc:
        raise HTTPException(404, "Budget allocation not found")
    return alloc


@router.patch("/{dataset_id}/{analyst_id}", response_model=schemas.BudgetAllocationRead)
def update_allocation(
    dataset_id: str,
    analyst_id: str,
    data: schemas.BudgetAllocationUpdate,
    db: Session = Depends(get_db),
):
    alloc = budget_manager.update_allocation(db, dataset_id, analyst_id, data)
    if not alloc:
        raise HTTPException(404, "Budget allocation not found")
    return alloc


@router.post("/{dataset_id}/{analyst_id}/reset", response_model=schemas.BudgetAllocationRead)
def reset_allocation(dataset_id: str, analyst_id: str, db: Session = Depends(get_db)):
    alloc = budget_manager.reset_allocation(db, dataset_id, analyst_id)
    if not alloc:
        raise HTTPException(404, "Budget allocation not found")
    return alloc


@router.get("/{dataset_id}/{analyst_id}/summary", response_model=schemas.CompositionSummary)
def composition_summary(dataset_id: str, analyst_id: str, db: Session = Depends(get_db)):
    from ..query_planner import PlannerError
    try:
        return get_composition_summary(db, dataset_id, analyst_id)
    except PlannerError as e:
        raise HTTPException(404, str(e))


@router.get("/{dataset_id}/{analyst_id}/ledger", response_model=List[schemas.LedgerEntryRead])
def ledger_entries(dataset_id: str, analyst_id: str, db: Session = Depends(get_db)):
    return budget_manager.list_ledger_entries(db, dataset_id, analyst_id)
