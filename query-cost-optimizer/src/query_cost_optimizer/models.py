"""Shared data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class Platform(str, Enum):
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"


class Severity(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class RecommendationType(str, Enum):
    CLUSTERING = "clustering"
    PARTITIONING = "partitioning"
    EXPENSIVE_PATTERN = "expensive_pattern"
    MATERIALIZATION = "materialization"


@dataclass
class QueryRecord:
    query_id: str
    query_text: str
    user: str
    start_time: datetime
    end_time: datetime
    bytes_processed: int
    bytes_billed: int
    elapsed_ms: int
    tables_referenced: list[str] = field(default_factory=list)
    platform: Platform = Platform.BIGQUERY
    cost_usd: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TableStats:
    table_id: str
    platform: Platform
    row_count: int = 0
    size_bytes: int = 0
    query_count: int = 0
    total_bytes_scanned: int = 0
    total_cost_usd: float = 0.0
    columns: list[str] = field(default_factory=list)
    filter_columns: list[str] = field(default_factory=list)
    join_columns: list[str] = field(default_factory=list)
    order_by_columns: list[str] = field(default_factory=list)
    group_by_columns: list[str] = field(default_factory=list)


@dataclass
class Recommendation:
    rec_type: RecommendationType
    platform: Platform
    severity: Severity
    table_id: str
    title: str
    description: str
    action: str
    estimated_savings_usd_monthly: float
    affected_query_count: int
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExpensivePattern:
    pattern_name: str
    platform: Platform
    severity: Severity
    description: str
    query_count: int
    total_cost_usd: float
    estimated_savings_pct: float
    example_queries: list[str] = field(default_factory=list)
    fix_suggestion: str = ""

    @property
    def estimated_savings_usd(self) -> float:
        return self.total_cost_usd * (self.estimated_savings_pct / 100)


@dataclass
class AnalysisReport:
    platform: Platform
    generated_at: datetime
    history_days: int
    total_queries_analyzed: int
    total_cost_usd: float
    total_bytes_processed: int
    recommendations: list[Recommendation] = field(default_factory=list)
    expensive_patterns: list[ExpensivePattern] = field(default_factory=list)
    top_tables: list[TableStats] = field(default_factory=list)

    @property
    def total_estimated_savings_usd(self) -> float:
        rec_savings = sum(r.estimated_savings_usd_monthly for r in self.recommendations)
        pattern_savings = sum(p.estimated_savings_usd for p in self.expensive_patterns)
        return rec_savings + pattern_savings
