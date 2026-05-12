from sqlalchemy import Column, String, Float, Integer, DateTime, Enum as SAEnum, ForeignKey, Boolean, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid

from .database import Base
from .privacy_mechanisms import Mechanism, QueryType


def new_uuid() -> str:
    return str(uuid.uuid4())


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


class Analyst(Base):
    __tablename__ = "analysts"

    id = Column(String, primary_key=True, default=new_uuid)
    username = Column(String, nullable=False, unique=True)
    email = Column(String, nullable=False, unique=True)
    role = Column(String, default="analyst")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    budgets = relationship("BudgetAllocation", back_populates="analyst", cascade="all, delete-orphan")
    query_logs = relationship("QueryLog", back_populates="analyst", cascade="all, delete-orphan")


class BudgetAllocation(Base):
    __tablename__ = "budget_allocations"

    id = Column(String, primary_key=True, default=new_uuid)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    analyst_id = Column(String, ForeignKey("analysts.id"), nullable=False)

    total_epsilon = Column(Float, nullable=False)          # allocated budget
    consumed_epsilon = Column(Float, default=0.0)          # spent so far
    total_delta = Column(Float, default=1e-5)
    consumed_delta = Column(Float, default=0.0)

    # What to do when budget is exhausted
    exhaustion_policy = Column(String, default="block")    # "block" | "inject_noise"
    default_mechanism = Column(SAEnum(Mechanism), default=Mechanism.LAPLACE)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    dataset = relationship("Dataset", back_populates="budgets")
    analyst = relationship("Analyst", back_populates="budgets")

    @property
    def remaining_epsilon(self) -> float:
        return max(0.0, self.total_epsilon - self.consumed_epsilon)

    @property
    def is_exhausted(self) -> bool:
        return self.consumed_epsilon >= self.total_epsilon


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

    status = Column(String, nullable=False)   # "allowed" | "noised" | "blocked"
    budget_remaining_after = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    dataset = relationship("Dataset", back_populates="query_logs")
    analyst = relationship("Analyst", back_populates="query_logs")
