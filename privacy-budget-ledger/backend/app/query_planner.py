"""
Query Planner: the gatekeeper between analysts and the privacy ledger.

For every incoming query the planner:
  1. Loads the current composition state for the (dataset, analyst) pair
  2. Evaluates the proposed query under all three accountants
  3. Returns one of three decisions:
       accept  — query fits as-is under tight RDP/zCDP accounting
       rewrite — query exceeds budget at requested ε but can be admitted
                 with higher noise (larger σ / smaller ε)
       reject  — no amount of additional noise can save this query

The planner never mutates the DB — that is the budget_manager's job.
"""
from __future__ import annotations

import math
from typing import Optional

from sqlalchemy.orm import Session

from . import models, schemas
from .composition import (
    ALPHA_ORDERS,
    BudgetAllocationSpec,
    CompositionLedger,
    CompositionState,
    QueryCost,
    QueryPlan,
    make_query_cost_gaussian,
    make_query_cost_laplace,
    current_dp_epsilon,
    zcdp_to_dp,
    rdp_curve_for_gaussian,
    rdp_curve_for_laplace,
    projected_dp_epsilon,
    zcdp_gaussian,
    zcdp_laplace_approx,
)
from .mechanisms import Mechanism, default_sensitivity


# ---------------------------------------------------------------------------
# Build a ledger from a DB BudgetAllocation row
# ---------------------------------------------------------------------------

def _load_ledger(db: Session, alloc: models.BudgetAllocation) -> CompositionLedger:
    """
    Reconstruct a CompositionLedger from the persisted DB columns.
    We persist accumulated_rdp_json as a list-of-pairs so we don't need
    to replay every LedgerEntry on startup.
    """
    import json as _json

    spec = BudgetAllocationSpec(
        total_epsilon=alloc.total_epsilon,
        total_delta=alloc.total_delta,
        total_rho=alloc.total_rho,
        exhaustion_policy=alloc.exhaustion_policy,
    )
    ledger = CompositionLedger(spec)

    # Restore persisted state directly (don't replay history)
    rdp_raw = alloc.accumulated_rdp_json or []
    if rdp_raw:
        ledger.state.accumulated_rdp = [(float(a), float(e)) for a, e in rdp_raw]
    else:
        ledger.state.accumulated_rdp = [(a, 0.0) for a in ALPHA_ORDERS]

    ledger.state.consumed_epsilon_basic = alloc.consumed_epsilon_basic or 0.0
    ledger.state.consumed_delta_basic = alloc.consumed_delta or 0.0
    ledger.state.consumed_rho = alloc.consumed_rho or 0.0
    return ledger


def _save_ledger_state(
    db: Session,
    alloc: models.BudgetAllocation,
    ledger: CompositionLedger,
    committed_cost: QueryCost,
) -> None:
    """Persist the updated composition state back to the BudgetAllocation row."""
    alloc.consumed_epsilon_basic = ledger.state.consumed_epsilon_basic
    alloc.consumed_delta = ledger.state.consumed_delta_basic
    alloc.consumed_rho = ledger.state.consumed_rho
    alloc.accumulated_rdp_json = list(ledger.state.accumulated_rdp)
    db.add(alloc)


# ---------------------------------------------------------------------------
# Build QueryCost from a request
# ---------------------------------------------------------------------------

def _make_cost(
    req: schemas.PlanRequest | schemas.QueryRequest,
    mechanism: Mechanism,
    sensitivity: float,
    sigma_override: Optional[float] = None,
) -> QueryCost:
    if mechanism == Mechanism.GAUSSIAN:
        delta = getattr(req, "delta_requested", 1e-5) or 1e-5
        if sigma_override is not None:
            sigma = sigma_override
        else:
            from .mechanisms.noise import calibrate_gaussian_sigma
            sigma = calibrate_gaussian_sigma(sensitivity, req.epsilon_requested, delta)
        return make_query_cost_gaussian(sensitivity, sigma, delta)
    else:
        return make_query_cost_laplace(sensitivity, req.epsilon_requested)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class PlannerError(Exception):
    pass


def get_allocation_or_raise(
    db: Session, dataset_id: str, analyst_id: str
) -> models.BudgetAllocation:
    alloc = (
        db.query(models.BudgetAllocation)
        .filter_by(dataset_id=dataset_id, analyst_id=analyst_id)
        .first()
    )
    if not alloc:
        raise PlannerError(
            f"No budget allocation for analyst {analyst_id} on dataset {dataset_id}. "
            "Ask a data owner to create one first."
        )
    return alloc


def plan_query(
    db: Session, req: schemas.PlanRequest
) -> schemas.PlanResponse:
    """
    Dry-run: evaluate a query request and return a plan without mutating state.
    """
    alloc = get_allocation_or_raise(db, req.dataset_id, req.analyst_id)
    ledger = _load_ledger(db, alloc)

    dataset = db.query(models.Dataset).filter_by(id=req.dataset_id).first()
    sensitivity = req.sensitivity or (dataset.sensitivity if dataset else 1.0)
    mechanism = req.mechanism or alloc.default_mechanism

    cost = _make_cost(req, mechanism, sensitivity)
    qplan: QueryPlan = ledger.plan_query(cost)

    # Current state for context
    cur_rdp = ledger.consumed_dp_epsilon_rdp()
    cur_zcdp = ledger.consumed_dp_epsilon_zcdp()
    cur_basic = ledger.consumed_dp_epsilon_basic()

    return schemas.PlanResponse(
        decision=qplan.decision,
        epsilon_requested=qplan.epsilon_requested,
        epsilon_feasible=qplan.epsilon_feasible,
        sigma_feasible=qplan.sigma_feasible,
        projected_epsilon_basic=qplan.projected_epsilon_basic,
        projected_epsilon_rdp=qplan.projected_epsilon_rdp,
        projected_epsilon_zcdp=qplan.projected_epsilon_zcdp,
        savings_vs_basic=qplan.savings_vs_basic,
        current_epsilon_basic=cur_basic,
        current_epsilon_rdp=cur_rdp,
        current_epsilon_zcdp=cur_zcdp,
        total_epsilon=alloc.total_epsilon,
        explanation=qplan.explanation,
    )


def execute_query(
    db: Session, req: schemas.QueryRequest
) -> schemas.QueryResponse:
    """
    Execute a query:
      1. Run the planner to get a decision
      2. If accept/rewrite: apply noise, debit budget, persist ledger entry
      3. If reject: return blocked response (no noise, no debit)
    """
    alloc = get_allocation_or_raise(db, req.dataset_id, req.analyst_id)
    ledger = _load_ledger(db, alloc)

    dataset = db.query(models.Dataset).filter_by(id=req.dataset_id).first()
    sensitivity = req.sensitivity or (dataset.sensitivity if dataset else 1.0)
    mechanism = req.mechanism or alloc.default_mechanism

    # Build initial cost (at requested ε)
    cost = _make_cost(req, mechanism, sensitivity)

    # Plan
    plan_req = schemas.PlanRequest(
        dataset_id=req.dataset_id,
        analyst_id=req.analyst_id,
        query_type=req.query_type,
        query_text=req.query_text,
        epsilon_requested=req.epsilon_requested,
        delta_requested=req.delta_requested,
        sensitivity=sensitivity,
        mechanism=mechanism,
    )
    qplan: QueryPlan = ledger.plan_query(cost)

    # ── Reject ─────────────────────────────────────────────────────────────
    if qplan.decision == "reject":
        log = _log_query(
            db, req, alloc.id, mechanism, sensitivity, None,
            "blocked", cost, qplan, None
        )
        return schemas.QueryResponse(
            query_id=log.id,
            status="blocked",
            result=None,
            noise_added=None,
            sigma_used=None,
            epsilon_requested=req.epsilon_requested,
            epsilon_consumed=0.0,
            planner_decision="reject",
            budget_remaining_basic=ledger.remaining_budget_rdp(),
            budget_remaining_rdp=ledger.remaining_budget_rdp(),
            budget_savings_vs_basic=ledger.savings_vs_basic(),
            mechanism_used=mechanism,
            message=qplan.explanation,
        )

    # ── Accept or Rewrite ───────────────────────────────────────────────────
    # If rewrite, use the feasible sigma / epsilon
    if qplan.decision == "rewrite":
        if qplan.sigma_feasible and mechanism == Mechanism.GAUSSIAN:
            cost = make_query_cost_gaussian(
                sensitivity, qplan.sigma_feasible, req.delta_requested or 1e-5
            )
        elif qplan.epsilon_feasible:
            cost = make_query_cost_laplace(sensitivity, qplan.epsilon_feasible)

    # Apply noise
    from .mechanisms.noise import apply_mechanism as _apply
    noisy, actual_cost = _apply(
        req.true_result,
        sensitivity,
        mechanism,
        qplan.epsilon_feasible or req.epsilon_requested,
        req.delta_requested or 1e-5,
        sigma_override=qplan.sigma_feasible if mechanism == Mechanism.GAUSSIAN else None,
    )
    noise = noisy - req.true_result

    # Commit to ledger
    ledger.commit_query(cost)

    # Persist state back to DB
    _save_ledger_state(db, alloc, ledger, cost)

    # Write LedgerEntry
    entry = models.LedgerEntry(
        allocation_id=alloc.id,
        mechanism=mechanism,
        sensitivity=sensitivity,
        sigma=cost.sigma,
        epsilon_basic=cost.epsilon_basic,
        delta_basic=cost.delta_basic,
        rho=cost.rho,
        projected_epsilon_basic=qplan.projected_epsilon_basic,
        projected_epsilon_rdp=qplan.projected_epsilon_rdp,
        projected_epsilon_zcdp=qplan.projected_epsilon_zcdp,
        savings_vs_basic=qplan.savings_vs_basic,
    )
    db.add(entry)
    db.flush()  # get entry.id before committing log

    log = _log_query(
        db, req, alloc.id, mechanism, sensitivity, entry.id,
        "allowed", cost, qplan, noisy,
    )
    entry.query_log_id = log.id
    db.commit()

    return schemas.QueryResponse(
        query_id=log.id,
        status="allowed",
        result=noisy,
        noise_added=noise,
        sigma_used=cost.sigma,
        epsilon_requested=req.epsilon_requested,
        epsilon_consumed=cost.epsilon_basic,
        planner_decision=qplan.decision,
        budget_remaining_basic=ledger.remaining_budget_rdp(),
        budget_remaining_rdp=ledger.remaining_budget_rdp(),
        budget_savings_vs_basic=ledger.savings_vs_basic(),
        mechanism_used=mechanism,
        message=qplan.explanation,
    )


def get_composition_summary(
    db: Session, dataset_id: str, analyst_id: str
) -> schemas.CompositionSummary:
    alloc = get_allocation_or_raise(db, dataset_id, analyst_id)
    ledger = _load_ledger(db, alloc)

    dataset = db.query(models.Dataset).filter_by(id=dataset_id).first()
    analyst = db.query(models.Analyst).filter_by(id=analyst_id).first()

    query_count = (
        db.query(models.QueryLog)
        .filter_by(dataset_id=dataset_id, analyst_id=analyst_id, status="allowed")
        .count()
    )

    cur_rdp = ledger.consumed_dp_epsilon_rdp()
    cur_zcdp = ledger.consumed_dp_epsilon_zcdp()

    return schemas.CompositionSummary(
        dataset_id=dataset_id,
        dataset_name=dataset.name if dataset else dataset_id,
        analyst_id=analyst_id,
        analyst_username=analyst.username if analyst else analyst_id,
        total_epsilon=alloc.total_epsilon,
        total_delta=alloc.total_delta,
        total_rho=alloc.total_rho,
        consumed_epsilon_basic=alloc.consumed_epsilon_basic,
        remaining_epsilon_basic=alloc.remaining_epsilon_basic,
        consumed_epsilon_rdp=cur_rdp,
        remaining_epsilon_rdp=ledger.remaining_budget_rdp(),
        consumed_epsilon_zcdp=cur_zcdp,
        remaining_epsilon_zcdp=ledger.remaining_budget_zcdp(),
        savings_epsilon=ledger.savings_vs_basic(),
        consumed_rho=alloc.consumed_rho,
        query_count=query_count,
        is_exhausted=alloc.is_exhausted_basic,
        exhaustion_policy=alloc.exhaustion_policy,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _log_query(
    db: Session,
    req: schemas.QueryRequest,
    allocation_id: str,
    mechanism: Mechanism,
    sensitivity: float,
    ledger_entry_id: Optional[str],
    status: str,
    cost: QueryCost,
    qplan: QueryPlan,
    noisy_result: Optional[float],
) -> models.QueryLog:
    alloc = (
        db.query(models.BudgetAllocation)
        .filter_by(dataset_id=req.dataset_id, analyst_id=req.analyst_id)
        .first()
    )
    log = models.QueryLog(
        dataset_id=req.dataset_id,
        analyst_id=req.analyst_id,
        query_type=req.query_type,
        query_text=req.query_text,
        true_result=req.true_result,
        noisy_result=noisy_result,
        noise_added=(noisy_result - req.true_result) if noisy_result is not None else None,
        epsilon_requested=req.epsilon_requested,
        delta_requested=req.delta_requested,
        mechanism_used=mechanism,
        sensitivity=sensitivity,
        sigma_used=cost.sigma,
        planner_decision=qplan.decision,
        epsilon_feasible=qplan.epsilon_feasible,
        status=status,
        budget_remaining_rdp=qplan.projected_epsilon_rdp if status == "allowed" else None,
        budget_remaining_basic=qplan.projected_epsilon_basic if status == "allowed" else None,
    )
    db.add(log)
    db.flush()
    return log
