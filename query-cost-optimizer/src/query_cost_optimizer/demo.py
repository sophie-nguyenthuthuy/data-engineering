"""Synthetic demo data so users can try the tool without credentials."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
import random

from .models import (
    AnalysisReport, ExpensivePattern, Platform, QueryRecord,
    Recommendation, RecommendationType, Severity, TableStats,
)


def build_demo_report(platform: str = "bigquery") -> AnalysisReport:
    plat = Platform(platform)
    now = datetime.now(timezone.utc)

    # ── synthetic tables ───────────────────────────────────────────────
    tables = [
        TableStats(
            table_id="myproject.analytics.events" if plat == Platform.BIGQUERY else "ANALYTICS.PUBLIC.EVENTS",
            platform=plat,
            row_count=2_400_000_000,
            size_bytes=1_200 * 1024 ** 3,
            query_count=3_421,
            total_bytes_scanned=980 * 1024 ** 4,
            total_cost_usd=5_975.0,
            filter_columns=["event_date", "user_id", "country", "event_type"],
            join_columns=["user_id"],
            group_by_columns=["event_date", "country"],
        ),
        TableStats(
            table_id="myproject.dw.orders" if plat == Platform.BIGQUERY else "DW.PUBLIC.ORDERS",
            platform=plat,
            row_count=180_000_000,
            size_bytes=90 * 1024 ** 3,
            query_count=1_102,
            total_bytes_scanned=72 * 1024 ** 4,
            total_cost_usd=450.0,
            filter_columns=["created_at", "status", "customer_id"],
            join_columns=["customer_id", "product_id"],
            group_by_columns=["created_at", "status"],
        ),
        TableStats(
            table_id="myproject.ml.feature_store" if plat == Platform.BIGQUERY else "ML.PUBLIC.FEATURE_STORE",
            platform=plat,
            row_count=50_000_000,
            size_bytes=25 * 1024 ** 3,
            query_count=680,
            total_bytes_scanned=18 * 1024 ** 4,
            total_cost_usd=112.5,
            filter_columns=["snapshot_date", "entity_id"],
            join_columns=["entity_id"],
            group_by_columns=["snapshot_date"],
        ),
    ]

    # ── recommendations ────────────────────────────────────────────────
    recs = [
        Recommendation(
            rec_type=RecommendationType.PARTITIONING,
            platform=plat,
            severity=Severity.HIGH,
            table_id=tables[0].table_id,
            title=f"Partition {tables[0].table_id.split('.')[-1]} by event_date",
            description=(
                "The events table is scanned without date-filter pruning, costing ~$5,975/period. "
                "Partitioning by event_date reduces full-table scans by ~30%."
            ),
            action=(
                "CREATE OR REPLACE TABLE `myproject.analytics.events`\n"
                "PARTITION BY DATE(event_date)\n"
                "AS SELECT * FROM `myproject.analytics.events`;"
            ) if plat == Platform.BIGQUERY else (
                "CREATE OR REPLACE TABLE ANALYTICS.PUBLIC.EVENTS\n"
                "PARTITION BY (event_date)\n"
                "AS SELECT * FROM ANALYTICS.PUBLIC.EVENTS;"
            ),
            estimated_savings_usd_monthly=1_792.5,
            affected_query_count=3_421,
            evidence={"suggested_partition_column": "event_date"},
        ),
        Recommendation(
            rec_type=RecommendationType.CLUSTERING,
            platform=plat,
            severity=Severity.HIGH,
            table_id=tables[0].table_id,
            title=f"Cluster {tables[0].table_id.split('.')[-1]} by user_id, country",
            description=(
                "Adding clustering keys (user_id, country) will improve filter pruning "
                "within partitions, saving an estimated additional 20% on scan cost."
            ),
            action=(
                "ALTER TABLE `myproject.analytics.events` CLUSTER BY user_id, country;"
            ) if plat == Platform.BIGQUERY else (
                "ALTER TABLE ANALYTICS.PUBLIC.EVENTS CLUSTER BY (user_id, country);"
            ),
            estimated_savings_usd_monthly=1_195.0,
            affected_query_count=2_890,
            evidence={"suggested_cluster_keys": ["user_id", "country"]},
        ),
        Recommendation(
            rec_type=RecommendationType.PARTITIONING,
            platform=plat,
            severity=Severity.MEDIUM,
            table_id=tables[1].table_id,
            title=f"Partition {tables[1].table_id.split('.')[-1]} by created_at",
            description=(
                "Orders are frequently filtered by created_at but the table is unpartitioned, "
                "causing full scans costing $450. Partitioning saves ~25%."
            ),
            action=(
                "CREATE OR REPLACE TABLE `myproject.dw.orders`\n"
                "PARTITION BY DATE(created_at)\n"
                "AS SELECT * FROM `myproject.dw.orders`;"
            ) if plat == Platform.BIGQUERY else (
                "CREATE OR REPLACE TABLE DW.PUBLIC.ORDERS\n"
                "PARTITION BY (created_at)\n"
                "AS SELECT * FROM DW.PUBLIC.ORDERS;"
            ),
            estimated_savings_usd_monthly=112.5,
            affected_query_count=1_102,
            evidence={"suggested_partition_column": "created_at"},
        ),
    ]

    # ── expensive patterns ─────────────────────────────────────────────
    patterns = [
        ExpensivePattern(
            pattern_name="SELECT * (full column scan)",
            platform=plat,
            severity=Severity.MEDIUM,
            description=(
                "874 queries use SELECT *, retrieving all columns from wide tables. "
                "In a columnar store this negates column pruning benefits."
            ),
            query_count=874,
            total_cost_usd=2_340.0,
            estimated_savings_pct=25,
            example_queries=[
                "SELECT * FROM `myproject.analytics.events` WHERE event_date = '2024-03-01'",
                "SELECT * FROM `myproject.dw.orders` WHERE status = 'pending'",
            ],
            fix_suggestion=(
                "Replace SELECT * with an explicit column list matching only the fields "
                "downstream consumers actually need."
            ),
        ),
        ExpensivePattern(
            pattern_name="Non-sargable filter (function on filter column)",
            platform=plat,
            severity=Severity.HIGH,
            description=(
                "312 queries apply functions to filter columns (e.g. DATE(ts) = '2024-01-01'), "
                "preventing partition pruning and forcing full scans."
            ),
            query_count=312,
            total_cost_usd=1_890.0,
            estimated_savings_pct=30,
            example_queries=[
                "SELECT user_id, revenue FROM events WHERE DATE(event_timestamp) = '2024-03-01'",
                "SELECT * FROM orders WHERE UPPER(status) = 'PENDING'",
            ],
            fix_suggestion=(
                "Use range predicates instead: event_timestamp >= '2024-03-01' AND "
                "event_timestamp < '2024-03-02'. For string comparisons normalise at ingestion."
            ),
        ),
        ExpensivePattern(
            pattern_name="ORDER BY without LIMIT",
            platform=plat,
            severity=Severity.LOW,
            description=(
                "203 queries sort millions of rows without a LIMIT, wasting sort compute "
                "when the full ordered result is never consumed."
            ),
            query_count=203,
            total_cost_usd=310.0,
            estimated_savings_pct=8,
            example_queries=[
                "SELECT user_id, SUM(revenue) FROM orders GROUP BY user_id ORDER BY SUM(revenue) DESC",
            ],
            fix_suggestion=(
                "Add LIMIT N for top-N use-cases, or remove ORDER BY entirely when "
                "downstream code does not depend on row order."
            ),
        ),
    ]

    return AnalysisReport(
        platform=plat,
        generated_at=now,
        history_days=30,
        total_queries_analyzed=12_450,
        total_cost_usd=8_320.0,
        total_bytes_processed=int(1_180 * 1024 ** 4),
        recommendations=recs,
        expensive_patterns=patterns,
        top_tables=tables,
    )
