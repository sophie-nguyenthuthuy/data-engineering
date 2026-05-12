"""
SQLAlchemy models.

New tables vs the original budget-manager:
  - LedgerEntry  — per-query composition record (RDP moments + ρ)
  - BudgetAllocation extended with total_rho / consumed_rho columns
"""
import uuid

from sqlalchemy import (
    Column, String, Float, Integer, DateTime, Boolean,
    Enum as SAEnum, ForeignKey, Text, JSON,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .database import Base
from .mechanisms import Mechanism, QueryType


def new_uuid() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(String, primary_key=True, default=new_uuid)
    name = Column(String, nullable=False, unique=True)
    description = Column(Text, default="")
    owner_id = Column(String, nullable=False)
    sensitivity = Column(Float, default=1.0)
    data_range_min = Column(Float, nullable=True)
    data_range_max = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    budgets = relationship("BudgetAllocation", back_populates="dataset", cascade="all, delete-orphan")
    query_logs = relationship("QueryLog", back_populates="dataset", cascade="all, delete-orphan")


# ---------------------------------------------------------------------------
# Analyst
# ---------------------------------------------------------------------------

class Analyst(Base):
    __tablename__ = "analysts"

    id = Column(String, primary_key=True, default=new_uuid)
    username = Column(String, nullable=False, unique=True)
    email = Column(String, nullable=False, unique=True)
    role = Column(String, default="analyst")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    budgets = relationship("BudgetAllocation", back_populates="analyst", cascade="all, delete-orphan")
    query_logs = relationship("QueryLog", back_populates="analyst", cascade="all, delete-orphan")


# ---------------------------------------------------------------------------
# BudgetAllocation  (extended with composition fields)
# ---------------------------------------------------------------------------

class BudgetAllocation(Base):
    __tablename__ = "budget_allocations"

    id = Column(String, primary_key=True, default=new_uuid)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    analyst_id = Column(String, ForeignKey("analysts.id"), nullable=False)

    # (ε, δ) budget — total cap for the analyst
    total_epsilon = Column(Float, nullable=False)
    consumed_epsilon_basic = Column(Float, default=0.0)   # naive Σε
    total_delta = Column(Float, default=1e-5)
    consumed_delta = Column(Float, default=0.0)

    # zCDP budget
    total_rho = Column(Float, nullable=True)              # None = derived from (ε,δ)
    consumed_rho = Column(Float, default=0.0)

    # RDP accumulated ε at each α order (stored as JSON list of [alpha, eps] pairs)
    accumulated_rdp_json = Column(JSON, default=list)

    exhaustion_policy = Column(String, default="block")   # "block" | "inject_noise"
    default_mechanism = Column(SAEnum(Mechanism), default=Mechanism.GAUSSIAN)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    dataset = relationship("Dataset", back_populates="budgets")
    analyst = relationship("Analyst", back_populates="budgets")
    ledger_entries = relationship("LedgerEntry", back_populates="allocation", cascade="all, delete-orphan")

    @property
    def remaining_epsilon_basic(self) -> float:
        return max(0.0, self.total_epsilon - self.consumed_epsilon_basic)

    @property
    def is_exhausted_basic(self) -> bool:
        return self.consumed_epsilon_basic >= self.total_epsilon


# ---------------------------------------------------------------------------
# LedgerEntry  — one row per admitted query, stores composition metadata
# ---------------------------------------------------------------------------

class LedgerEntry(Base):
    __tablename__ = "ledger_entries"

    id = Column(String, primary_key=True, default=new_uuid)
    allocation_id = Column(String, ForeignKey("budget_allocations.id"), nullable=False)
    query_log_id = Column(String, ForeignKey("query_logs.id"), nullable=True)

    mechanism = Column(SAEnum(Mechanism), nullable=False)
    sensitivity = Column(Float, nullable=False)
    sigma = Column(Float, nullable=True)             # Gaussian only
    noise_scale_b = Column(Float, nullable=True)     # Laplace only

    epsilon_basic = Column(Float, nullable=False)    # per-query ε contribution
    delta_basic = Column(Float, default=0.0)
    rho = Column(Float, default=0.0)                 # zCDP ρ

    # Snapshot: projected (ε,δ)-DP after this query under each accountant
    projected_epsilon_basic = Column(Float, nullable=True)
    projected_epsilon_rdp = Column(Float, nullable=True)
    projected_epsilon_zcdp = Column(Float, nullable=True)
    savings_vs_basic = Column(Float, nullable=True)  # rdp_basic - rdp_tight

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    allocation = relationship("BudgetAllocation", back_populates="ledger_entries")
    query_log = relationship("QueryLog", back_populates="ledger_entry")


# ---------------------------------------------------------------------------
# QueryLog  — noisy result record
# ---------------------------------------------------------------------------

class QueryLog(Base):
    __tablename__ = "query_logs"

    id = Column(String, primary_key=True, default=new_uuid)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    analyst_id = Column(String, ForeignKey("analysts.id"), nullable=False)

    query_type = Column(SAEnum(QueryType), nullable=False)
    query_text = Column(Text, nullable=False)
    true_result = Column(Float, nullable=True)
    noisy_result = Column(Float, nullable=True)
    noise_added = Column(Float, nullable=True)

    epsilon_requested = Column(Float, nullable=False)
    delta_requested = Column(Float, default=0.0)
    mechanism_used = Column(SAEnum(Mechanism), nullable=False)
    sensitivity = Column(Float, nullable=False)
    sigma_used = Column(Float, nullable=True)

    # Decision from query planner
    planner_decision = Column(String, nullable=True)  # accept | rewrite | reject
    epsilon_feasible = Column(Float, nullable=True)   # actual ε after rewrite

    status = Column(String, nullable=False)           # "allowed" | "blocked"
    budget_remaining_rdp = Column(Float, nullable=True)
    budget_remaining_basic = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    dataset = relationship("Dataset", back_populates="query_logs")
    analyst = relationship("Analyst", back_populates="query_logs")
    ledger_entry = relationship("LedgerEntry", back_populates="query_log", uselist=False)
