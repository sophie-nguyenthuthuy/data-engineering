"""Orchestration layer — ties analyzers, recommenders, and reporters together."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Literal

from .models import AnalysisReport, Platform
from .recommenders.clustering import ClusteringRecommender
from .recommenders.partitioning import PartitioningRecommender
from .recommenders.patterns import PatternDetector

logger = logging.getLogger(__name__)


def run_bigquery(
    project_id: str | None = None,
    history_days: int = 30,
    min_savings_usd: float = 10.0,
    min_query_count: int = 5,
) -> AnalysisReport:
    """Full BigQuery analysis pipeline. Returns an AnalysisReport."""
    from .analyzers.bigquery import BigQueryAnalyzer

    analyzer = BigQueryAnalyzer(project_id=project_id, history_days=history_days)
    records = analyzer.fetch_query_history()
    table_stats = analyzer.build_table_stats(records)

    clustering_recs = ClusteringRecommender(min_query_count=min_query_count).recommend(table_stats)
    partition_recs = PartitioningRecommender(min_query_count=min_query_count).recommend(table_stats)
    patterns = PatternDetector(min_query_count=min_query_count).detect(records)

    all_recs = sorted(
        [r for r in clustering_recs + partition_recs if r.estimated_savings_usd_monthly >= min_savings_usd],
        key=lambda r: r.estimated_savings_usd_monthly,
        reverse=True,
    )

    return AnalysisReport(
        platform=Platform.BIGQUERY,
        generated_at=datetime.now(timezone.utc),
        history_days=history_days,
        total_queries_analyzed=len(records),
        total_cost_usd=sum(r.cost_usd for r in records),
        total_bytes_processed=sum(r.bytes_processed for r in records),
        recommendations=all_recs,
        expensive_patterns=[p for p in patterns if p.estimated_savings_usd >= min_savings_usd],
        top_tables=table_stats[:20],
    )


def run_snowflake(
    account: str | None = None,
    user: str | None = None,
    password: str | None = None,
    warehouse: str | None = None,
    history_days: int = 30,
    min_savings_usd: float = 10.0,
    min_query_count: int = 5,
) -> AnalysisReport:
    """Full Snowflake analysis pipeline. Returns an AnalysisReport."""
    from .analyzers.snowflake import SnowflakeAnalyzer

    analyzer = SnowflakeAnalyzer(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        history_days=history_days,
    )
    try:
        records = analyzer.fetch_query_history()
        table_stats = analyzer.build_table_stats(records)
    finally:
        analyzer.close()

    clustering_recs = ClusteringRecommender(min_query_count=min_query_count).recommend(table_stats)
    partition_recs = PartitioningRecommender(min_query_count=min_query_count).recommend(table_stats)
    patterns = PatternDetector(min_query_count=min_query_count).detect(records)

    all_recs = sorted(
        [r for r in clustering_recs + partition_recs if r.estimated_savings_usd_monthly >= min_savings_usd],
        key=lambda r: r.estimated_savings_usd_monthly,
        reverse=True,
    )

    return AnalysisReport(
        platform=Platform.SNOWFLAKE,
        generated_at=datetime.now(timezone.utc),
        history_days=history_days,
        total_queries_analyzed=len(records),
        total_cost_usd=sum(r.cost_usd for r in records),
        total_bytes_processed=sum(r.bytes_processed for r in records),
        recommendations=all_recs,
        expensive_patterns=[p for p in patterns if p.estimated_savings_usd >= min_savings_usd],
        top_tables=table_stats[:20],
    )
