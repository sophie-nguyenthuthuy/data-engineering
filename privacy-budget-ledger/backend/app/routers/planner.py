"""
Query planner endpoints.

  POST /planner/plan    — dry-run: get decision without executing the query
  POST /planner/execute — execute a query through the planner gateway
  GET  /planner/logs    — query log with planner decisions
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from ..database import get_db
from .. import schemas, budget_manager
from ..query_planner import plan_query, execute_query, PlannerError

router = APIRouter(prefix="/planner", tags=["planner"])


@router.post("/plan", response_model=schemas.PlanResponse)
def plan(req: schemas.PlanRequest, db: Session = Depends(get_db)):
    """
    Dry-run: evaluate whether a query fits under tight RDP/zCDP accounting.
    Returns decision (accept/rewrite/reject) and budget projections.
    No state is modified.
    """
    try:
        return plan_query(db, req)
    except PlannerError as e:
        raise HTTPException(404, str(e))


@router.post("/execute", response_model=schemas.QueryResponse, status_code=200)
def execute(req: schemas.QueryRequest, db: Session = Depends(get_db)):
    """
    Execute a query through the planner gateway.
    - accept:  runs as-is, debits budget
    - rewrite: runs with adjusted (higher) noise to fit within budget
    - reject:  returns 403 with explanation
    """
    try:
        resp = execute_query(db, req)
    except PlannerError as e:
        raise HTTPException(404, str(e))

    if resp.status == "blocked":
        raise HTTPException(
            status_code=403,
            detail={
                "decision": resp.planner_decision,
                "message": resp.message,
                "budget_remaining_rdp": resp.budget_remaining_rdp,
            },
        )
    return resp


@router.get("/logs", response_model=List[schemas.QueryLogRead])
def query_logs(
    dataset_id: Optional[str] = None,
    analyst_id: Optional[str] = None,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    logs = budget_manager.list_query_logs(db, dataset_id, analyst_id, limit)
    # Map to schema: epsilon_consumed = epsilon_feasible (actual charged)
    result = []
    for log in logs:
        result.append(schemas.QueryLogRead(
            id=log.id,
            dataset_id=log.dataset_id,
            analyst_id=log.analyst_id,
            query_type=log.query_type,
            query_text=log.query_text,
            noisy_result=log.noisy_result,
            epsilon_requested=log.epsilon_requested,
            epsilon_consumed=log.epsilon_feasible or log.epsilon_requested,
            mechanism_used=log.mechanism_used,
            planner_decision=log.planner_decision,
            status=log.status,
            budget_remaining_rdp=log.budget_remaining_rdp,
            created_at=log.created_at,
        ))
    return result
