"""
Budget manager: CRUD operations for datasets, analysts, and budget allocations.
Query execution and planning are handled by query_planner.py.
"""
from __future__ import annotations

from typing import List, Optional

from sqlalchemy.orm import Session

from . import models, schemas
from .composition import rho_for_dp_target


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------

def create_dataset(db: Session, data: schemas.DatasetCreate) -> models.Dataset:
    ds = models.Dataset(**data.model_dump())
    db.add(ds)
    db.commit()
    db.refresh(ds)
    return ds


def list_datasets(db: Session) -> List[models.Dataset]:
    return db.query(models.Dataset).all()


def get_dataset(db: Session, dataset_id: str) -> Optional[models.Dataset]:
    return db.query(models.Dataset).filter_by(id=dataset_id).first()


def delete_dataset(db: Session, dataset_id: str) -> bool:
    ds = get_dataset(db, dataset_id)
    if not ds:
        return False
    db.delete(ds)
    db.commit()
    return True


# ---------------------------------------------------------------------------
# Analysts
# ---------------------------------------------------------------------------

def create_analyst(db: Session, data: schemas.AnalystCreate) -> models.Analyst:
    a = models.Analyst(**data.model_dump())
    db.add(a)
    db.commit()
    db.refresh(a)
    return a


def list_analysts(db: Session) -> List[models.Analyst]:
    return db.query(models.Analyst).all()


def get_analyst(db: Session, analyst_id: str) -> Optional[models.Analyst]:
    return db.query(models.Analyst).filter_by(id=analyst_id).first()


def delete_analyst(db: Session, analyst_id: str) -> bool:
    a = get_analyst(db, analyst_id)
    if not a:
        return False
    db.delete(a)
    db.commit()
    return True


# ---------------------------------------------------------------------------
# Budget Allocations
# ---------------------------------------------------------------------------

def create_allocation(
    db: Session, data: schemas.BudgetAllocationCreate
) -> models.BudgetAllocation:
    # Derive total_rho from (ε,δ) if not provided
    total_rho = data.total_rho
    if total_rho is None:
        total_rho = rho_for_dp_target(data.total_epsilon, data.total_delta)

    alloc = models.BudgetAllocation(
        dataset_id=data.dataset_id,
        analyst_id=data.analyst_id,
        total_epsilon=data.total_epsilon,
        total_delta=data.total_delta,
        total_rho=total_rho,
        exhaustion_policy=data.exhaustion_policy,
        default_mechanism=data.default_mechanism,
        consumed_epsilon_basic=0.0,
        consumed_delta=0.0,
        consumed_rho=0.0,
        accumulated_rdp_json=[],
    )
    db.add(alloc)
    db.commit()
    db.refresh(alloc)
    return alloc


def get_allocation(
    db: Session, dataset_id: str, analyst_id: str
) -> Optional[models.BudgetAllocation]:
    return (
        db.query(models.BudgetAllocation)
        .filter_by(dataset_id=dataset_id, analyst_id=analyst_id)
        .first()
    )


def list_allocations(db: Session) -> List[models.BudgetAllocation]:
    return db.query(models.BudgetAllocation).all()


def update_allocation(
    db: Session,
    dataset_id: str,
    analyst_id: str,
    data: schemas.BudgetAllocationUpdate,
) -> Optional[models.BudgetAllocation]:
    alloc = get_allocation(db, dataset_id, analyst_id)
    if not alloc:
        return None
    for field, val in data.model_dump(exclude_none=True).items():
        setattr(alloc, field, val)
    db.commit()
    db.refresh(alloc)
    return alloc


def reset_allocation(
    db: Session, dataset_id: str, analyst_id: str
) -> Optional[models.BudgetAllocation]:
    """Reset consumed budget counters (for testing or data owner override)."""
    alloc = get_allocation(db, dataset_id, analyst_id)
    if not alloc:
        return None
    alloc.consumed_epsilon_basic = 0.0
    alloc.consumed_delta = 0.0
    alloc.consumed_rho = 0.0
    alloc.accumulated_rdp_json = []
    db.commit()
    db.refresh(alloc)
    return alloc


# ---------------------------------------------------------------------------
# Ledger entries
# ---------------------------------------------------------------------------

def list_ledger_entries(
    db: Session, dataset_id: str, analyst_id: str
) -> List[models.LedgerEntry]:
    alloc = get_allocation(db, dataset_id, analyst_id)
    if not alloc:
        return []
    return (
        db.query(models.LedgerEntry)
        .filter_by(allocation_id=alloc.id)
        .order_by(models.LedgerEntry.created_at)
        .all()
    )


# ---------------------------------------------------------------------------
# Query logs
# ---------------------------------------------------------------------------

def list_query_logs(
    db: Session,
    dataset_id: Optional[str] = None,
    analyst_id: Optional[str] = None,
    limit: int = 100,
) -> List[models.QueryLog]:
    q = db.query(models.QueryLog)
    if dataset_id:
        q = q.filter_by(dataset_id=dataset_id)
    if analyst_id:
        q = q.filter_by(analyst_id=analyst_id)
    return q.order_by(models.QueryLog.created_at.desc()).limit(limit).all()
