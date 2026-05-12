from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Tuple

from pydantic import BaseModel, Field

from .mechanisms import Mechanism, QueryType


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class DatasetCreate(BaseModel):
    name: str
    description: str = ""
    owner_id: str
    sensitivity: float = 1.0
    data_range_min: Optional[float] = None
    data_range_max: Optional[float] = None


class DatasetRead(DatasetCreate):
    id: str
    created_at: datetime
    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Analyst
# ---------------------------------------------------------------------------

class AnalystCreate(BaseModel):
    username: str
    email: str
    role: str = "analyst"


class AnalystRead(AnalystCreate):
    id: str
    created_at: datetime
    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Budget Allocation
# ---------------------------------------------------------------------------

class BudgetAllocationCreate(BaseModel):
    dataset_id: str
    analyst_id: str
    total_epsilon: float = Field(gt=0, description="Total (ε,δ)-DP budget cap")
    total_delta: float = Field(default=1e-5, ge=0, lt=1)
    total_rho: Optional[float] = Field(default=None, gt=0, description="Explicit ρ-zCDP cap; derived from (ε,δ) if omitted")
    exhaustion_policy: str = Field(default="block", pattern="^(block|inject_noise)$")
    default_mechanism: Mechanism = Mechanism.GAUSSIAN


class BudgetAllocationRead(BudgetAllocationCreate):
    id: str
    consumed_epsilon_basic: float
    consumed_rho: float
    consumed_delta: float
    remaining_epsilon_basic: float
    is_exhausted_basic: bool
    created_at: datetime
    updated_at: Optional[datetime] = None
    model_config = {"from_attributes": True}


class BudgetAllocationUpdate(BaseModel):
    total_epsilon: Optional[float] = Field(default=None, gt=0)
    total_delta: Optional[float] = Field(default=None, ge=0, lt=1)
    total_rho: Optional[float] = Field(default=None, gt=0)
    exhaustion_policy: Optional[str] = Field(default=None, pattern="^(block|inject_noise)$")
    default_mechanism: Optional[Mechanism] = None


# ---------------------------------------------------------------------------
# Query Request / Response
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    dataset_id: str
    analyst_id: str
    query_type: QueryType
    query_text: str = Field(description="Human-readable description")
    true_result: float = Field(description="True (unnoised) query answer")
    epsilon_requested: float = Field(gt=0, description="Desired privacy budget for this query")
    delta_requested: float = Field(default=1e-5, ge=0, lt=1)
    sensitivity: Optional[float] = Field(default=None, gt=0)
    mechanism: Optional[Mechanism] = None
    # If True, use RDP/zCDP-tighter accounting; if False, use basic ε-composition
    use_tight_accounting: bool = True


class QueryResponse(BaseModel):
    query_id: str
    status: str                        # "allowed" | "blocked"
    result: Optional[float]
    noise_added: Optional[float]
    sigma_used: Optional[float]

    epsilon_requested: float
    epsilon_consumed: float            # actual ε charged (may differ if rewritten)
    planner_decision: str              # "accept" | "rewrite" | "reject"

    budget_remaining_basic: float
    budget_remaining_rdp: float
    budget_savings_vs_basic: float     # how much tighter accounting saved

    mechanism_used: Mechanism
    message: str
    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Planner (dry-run)
# ---------------------------------------------------------------------------

class PlanRequest(BaseModel):
    dataset_id: str
    analyst_id: str
    query_type: QueryType
    query_text: str = ""
    epsilon_requested: float = Field(gt=0)
    delta_requested: float = Field(default=1e-5, ge=0, lt=1)
    sensitivity: Optional[float] = None
    mechanism: Mechanism = Mechanism.GAUSSIAN


class PlanResponse(BaseModel):
    decision: str                      # "accept" | "rewrite" | "reject"
    epsilon_requested: float
    epsilon_feasible: Optional[float]
    sigma_feasible: Optional[float]

    # Budget projections after query
    projected_epsilon_basic: float
    projected_epsilon_rdp: float
    projected_epsilon_zcdp: float
    savings_vs_basic: Optional[float]

    # Current accumulated budget
    current_epsilon_basic: float
    current_epsilon_rdp: float
    current_epsilon_zcdp: float
    total_epsilon: float

    explanation: str


# ---------------------------------------------------------------------------
# Ledger Entry (read-only)
# ---------------------------------------------------------------------------

class LedgerEntryRead(BaseModel):
    id: str
    allocation_id: str
    mechanism: Mechanism
    sensitivity: float
    sigma: Optional[float]
    epsilon_basic: float
    rho: float
    projected_epsilon_basic: Optional[float]
    projected_epsilon_rdp: Optional[float]
    projected_epsilon_zcdp: Optional[float]
    savings_vs_basic: Optional[float]
    created_at: datetime
    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Composition Summary (dashboard)
# ---------------------------------------------------------------------------

class CompositionSummary(BaseModel):
    dataset_id: str
    dataset_name: str
    analyst_id: str
    analyst_username: str

    total_epsilon: float
    total_delta: float
    total_rho: Optional[float]

    # Basic composition (pessimistic)
    consumed_epsilon_basic: float
    remaining_epsilon_basic: float

    # RDP tight bound
    consumed_epsilon_rdp: float
    remaining_epsilon_rdp: float

    # zCDP tight bound
    consumed_epsilon_zcdp: float
    remaining_epsilon_zcdp: float

    # Budget savings (how much tighter accounting helps)
    savings_epsilon: float             # basic - rdp_tight

    consumed_rho: float
    query_count: int
    is_exhausted: bool
    exhaustion_policy: str


class QueryLogRead(BaseModel):
    id: str
    dataset_id: str
    analyst_id: str
    query_type: QueryType
    query_text: str
    noisy_result: Optional[float]
    epsilon_requested: float
    epsilon_consumed: Optional[float]
    mechanism_used: Mechanism
    planner_decision: Optional[str]
    status: str
    budget_remaining_rdp: Optional[float]
    created_at: datetime
    model_config = {"from_attributes": True}
