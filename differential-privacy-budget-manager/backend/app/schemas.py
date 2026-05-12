from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List
from datetime import datetime
from .privacy_mechanisms import Mechanism, QueryType


# ── Dataset ──────────────────────────────────────────────────────────────────

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


# ── Analyst ───────────────────────────────────────────────────────────────────

class AnalystCreate(BaseModel):
    username: str
    email: str
    role: str = "analyst"


class AnalystRead(AnalystCreate):
    id: str
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Budget Allocation ─────────────────────────────────────────────────────────

class BudgetAllocationCreate(BaseModel):
    dataset_id: str
    analyst_id: str
    total_epsilon: float = Field(gt=0, description="Total privacy budget (ε) to grant")
    total_delta: float = Field(default=1e-5, ge=0, lt=1)
    exhaustion_policy: str = Field(default="block", pattern="^(block|inject_noise)$")
    default_mechanism: Mechanism = Mechanism.LAPLACE


class BudgetAllocationRead(BudgetAllocationCreate):
    id: str
    consumed_epsilon: float
    consumed_delta: float
    remaining_epsilon: float
    is_exhausted: bool
    created_at: datetime
    updated_at: Optional[datetime]

    model_config = {"from_attributes": True}


class BudgetAllocationUpdate(BaseModel):
    total_epsilon: Optional[float] = Field(default=None, gt=0)
    total_delta: Optional[float] = Field(default=None, ge=0, lt=1)
    exhaustion_policy: Optional[str] = Field(default=None, pattern="^(block|inject_noise)$")
    default_mechanism: Optional[Mechanism] = None


# ── Query ─────────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    dataset_id: str
    analyst_id: str
    query_type: QueryType
    query_text: str = Field(description="Human-readable description of the query")
    true_result: float = Field(description="The true (unnoised) query result")
    epsilon_requested: float = Field(gt=0, description="Privacy budget this query should consume")
    delta_requested: float = Field(default=0.0, ge=0, lt=1)
    sensitivity: Optional[float] = Field(default=None, gt=0)
    mechanism: Optional[Mechanism] = None  # falls back to allocation default


class QueryResponse(BaseModel):
    query_id: str
    status: str          # "allowed" | "noised" | "blocked"
    result: Optional[float]
    noise_added: Optional[float]
    epsilon_consumed: float
    budget_remaining: float
    mechanism_used: Mechanism
    message: str

    model_config = {"from_attributes": True}


# ── Query Log ─────────────────────────────────────────────────────────────────

class QueryLogRead(BaseModel):
    id: str
    dataset_id: str
    analyst_id: str
    query_type: QueryType
    query_text: str
    noisy_result: Optional[float]
    epsilon_requested: float
    mechanism_used: Mechanism
    status: str
    budget_remaining_after: Optional[float]
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Dashboard ─────────────────────────────────────────────────────────────────

class BudgetSummary(BaseModel):
    dataset_id: str
    dataset_name: str
    analyst_id: str
    analyst_username: str
    total_epsilon: float
    consumed_epsilon: float
    remaining_epsilon: float
    percent_used: float
    is_exhausted: bool
    query_count: int
    exhaustion_policy: str


class DatasetBudgetOverview(BaseModel):
    dataset: DatasetRead
    allocations: List[BudgetSummary]
    total_analysts: int
    exhausted_count: int
