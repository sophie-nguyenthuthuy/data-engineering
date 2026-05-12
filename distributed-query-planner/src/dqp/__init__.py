"""Distributed Query Planner — federated predicate pushdown across MongoDB, Parquet, PostgreSQL."""
from __future__ import annotations

from dqp.catalog import Catalog, ColumnSchema, TableSchema
from dqp.cost.model import CostModel, PlanCost, estimate_selectivity
from dqp.cost.statistics import ColumnStats, Histogram, StatsRegistry, TableStats
from dqp.logical_plan import (
    AggregateNode,
    FilterNode,
    JoinNode,
    PlanNode,
    ProjectNode,
    PushedScanNode,
    ScanNode,
    plan_to_str,
)
from dqp.optimizer import FederatedOptimizer
from dqp.predicate import (
    AndPredicate,
    BetweenPredicate,
    ColumnRef,
    ComparisonOp,
    ComparisonPredicate,
    InPredicate,
    IsNullPredicate,
    LikePredicate,
    Literal,
    NotPredicate,
    OrPredicate,
    Predicate,
    columns_referenced,
    conjuncts,
    negate,
)

__version__ = "0.1.0"

__all__ = [
    # Catalog
    "Catalog",
    "ColumnSchema",
    "TableSchema",
    # Cost
    "CostModel",
    "PlanCost",
    "estimate_selectivity",
    "ColumnStats",
    "Histogram",
    "StatsRegistry",
    "TableStats",
    # Logical plan
    "AggregateNode",
    "FilterNode",
    "JoinNode",
    "PlanNode",
    "ProjectNode",
    "PushedScanNode",
    "ScanNode",
    "plan_to_str",
    # Optimizer
    "FederatedOptimizer",
    # Predicates
    "AndPredicate",
    "BetweenPredicate",
    "ColumnRef",
    "ComparisonOp",
    "ComparisonPredicate",
    "InPredicate",
    "IsNullPredicate",
    "LikePredicate",
    "Literal",
    "NotPredicate",
    "OrPredicate",
    "Predicate",
    "columns_referenced",
    "conjuncts",
    "negate",
]
