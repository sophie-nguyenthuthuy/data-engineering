"""
Core budget management logic: check, debit, and enforce privacy budgets.
"""
from sqlalchemy.orm import Session
from . import models, schemas
from .privacy_mechanisms import (
    Mechanism, apply_mechanism, PrivacyParams, default_sensitivity
)


class BudgetExhaustedError(Exception):
    pass


class BudgetNotFoundError(Exception):
    pass


def get_or_raise(db: Session, dataset_id: str, analyst_id: str) -> models.BudgetAllocation:
    alloc = (
        db.query(models.BudgetAllocation)
        .filter_by(dataset_id=dataset_id, analyst_id=analyst_id)
        .first()
    )
    if not alloc:
        raise BudgetNotFoundError(
            f"No budget allocation for analyst {analyst_id} on dataset {dataset_id}"
        )
    return alloc


def execute_query(db: Session, req: schemas.QueryRequest) -> schemas.QueryResponse:
    alloc = get_or_raise(db, req.dataset_id, req.analyst_id)

    mechanism = req.mechanism or alloc.default_mechanism
    sensitivity = req.sensitivity or (
        db.query(models.Dataset).filter_by(id=req.dataset_id).first().sensitivity
    )

    # Determine how much budget this query actually costs.
    epsilon_cost = req.epsilon_requested
    delta_cost = req.delta_requested if mechanism == Mechanism.GAUSSIAN else 0.0

    # ── Exhaustion check ─────────────────────────────────────────────────────
    if alloc.is_exhausted:
        if alloc.exhaustion_policy == "block":
            log = _log_query(db, req, mechanism, sensitivity, "blocked", None, None, alloc.remaining_epsilon)
            return schemas.QueryResponse(
                query_id=log.id,
                status="blocked",
                result=None,
                noise_added=None,
                epsilon_consumed=0.0,
                budget_remaining=alloc.remaining_epsilon,
                mechanism_used=mechanism,
                message="Privacy budget exhausted. Query blocked.",
            )
        else:
            # inject_noise: use what's left (approaching ∞ noise)
            epsilon_cost = max(alloc.remaining_epsilon, 1e-9)

    # ── Apply partial budget if remaining < requested ─────────────────────────
    if alloc.remaining_epsilon < epsilon_cost:
        if alloc.exhaustion_policy == "block":
            log = _log_query(db, req, mechanism, sensitivity, "blocked", None, None, alloc.remaining_epsilon)
            return schemas.QueryResponse(
                query_id=log.id,
                status="blocked",
                result=None,
                noise_added=None,
                epsilon_consumed=0.0,
                budget_remaining=alloc.remaining_epsilon,
                mechanism_used=mechanism,
                message=f"Insufficient budget. Requested ε={epsilon_cost:.4f}, remaining ε={alloc.remaining_epsilon:.4f}.",
            )
        else:
            epsilon_cost = alloc.remaining_epsilon

    # ── Add noise and debit budget ────────────────────────────────────────────
    params = PrivacyParams(
        epsilon=epsilon_cost,
        delta=delta_cost,
        sensitivity=sensitivity,
        mechanism=mechanism,
    )
    noisy = apply_mechanism(req.true_result, params)
    noise = noisy - req.true_result

    alloc.consumed_epsilon = round(alloc.consumed_epsilon + epsilon_cost, 10)
    alloc.consumed_delta = round(alloc.consumed_delta + delta_cost, 10)
    db.commit()

    log = _log_query(
        db, req, mechanism, sensitivity, "allowed",
        noisy, noise, alloc.remaining_epsilon
    )

    return schemas.QueryResponse(
        query_id=log.id,
        status="allowed",
        result=noisy,
        noise_added=noise,
        epsilon_consumed=epsilon_cost,
        budget_remaining=alloc.remaining_epsilon,
        mechanism_used=mechanism,
        message="Query executed with differential privacy.",
    )


def _log_query(
    db: Session,
    req: schemas.QueryRequest,
    mechanism: Mechanism,
    sensitivity: float,
    status: str,
    noisy_result,
    noise_added,
    budget_remaining_after,
) -> models.QueryLog:
    log = models.QueryLog(
        dataset_id=req.dataset_id,
        analyst_id=req.analyst_id,
        query_type=req.query_type,
        query_text=req.query_text,
        true_result=req.true_result,
        noisy_result=noisy_result,
        noise_added=noise_added,
        epsilon_requested=req.epsilon_requested,
        delta_requested=req.delta_requested,
        mechanism_used=mechanism,
        sensitivity=sensitivity,
        status=status,
        budget_remaining_after=budget_remaining_after,
    )
    db.add(log)
    db.commit()
    db.refresh(log)
    return log
