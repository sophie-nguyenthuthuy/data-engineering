"""
Cost model — predicts monthly USD spend across all three tiers.

Pricing defaults reflect AWS us-east-1 as of 2025.  Override via CostConfig.

Formulae
--------
Redis (ElastiCache r7g.large, ~13 GB usable):
    $/GB/month  = instance_cost / usable_gb

Postgres (RDS db.t4g.medium, gp3 storage):
    $/GB/month  = storage_rate + iops_rate * iops_per_gb

S3 Standard (warm):
    $/GB/month  = s3_standard_rate
    + PUT/GET requests priced per 1 000

S3 Glacier Flexible Retrieval (cold):
    $/GB/month  = s3_glacier_rate
    + expedited/standard/bulk restore rates per GB retrieved

Rehydration:
    Estimated from cold-tier access frequency × restore cost per priority.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tiered_storage.schemas import (
    CostBreakdown,
    RehydrationPriority,
    TierMetrics,
)

if TYPE_CHECKING:
    pass


@dataclass
class CostConfig:
    # ---- Hot: Redis (ElastiCache r7g.large on-demand, us-east-1) ----
    redis_instance_usd_per_month: float = 130.0    # ~r7g.large
    redis_usable_gb: float = 12.0                  # usable RAM per node

    # ---- Hot: PostgreSQL (RDS db.t4g.medium + gp3 storage) ----------
    postgres_instance_usd_per_month: float = 60.0
    postgres_storage_usd_per_gb_month: float = 0.115  # gp3 base
    postgres_iops_usd_per_million: float = 0.02

    # ---- Warm: S3 Standard -------------------------------------------
    s3_standard_usd_per_gb_month: float = 0.023
    s3_put_usd_per_1k: float = 0.005
    s3_get_usd_per_1k: float = 0.0004
    s3_select_usd_per_gb_scanned: float = 0.002

    # ---- Cold: S3 Glacier Flexible Retrieval -------------------------
    s3_glacier_usd_per_gb_month: float = 0.004
    # Restore costs per GB
    glacier_expedited_usd_per_gb: float = 0.03
    glacier_standard_usd_per_gb: float = 0.01
    glacier_bulk_usd_per_gb: float = 0.0025

    # ---- Data transfer (egress to internet) -------------------------
    egress_usd_per_gb: float = 0.09


@dataclass
class TierUsage:
    """Observed or projected usage figures fed into the cost model."""
    # Hot tier
    redis_used_gb: float = 0.0
    postgres_used_gb: float = 0.0
    hot_reads_per_day: float = 0.0
    hot_writes_per_day: float = 0.0

    # Warm tier
    warm_used_gb: float = 0.0
    warm_reads_per_day: float = 0.0     # GET requests
    warm_writes_per_day: float = 0.0    # PUT requests (promotions/demotions)

    # Cold tier
    cold_used_gb: float = 0.0
    cold_reads_per_day: float = 0.0     # rehydration requests
    cold_read_priority: RehydrationPriority = RehydrationPriority.STANDARD

    # Egress
    egress_gb_per_day: float = 0.0


class CostModel:
    """
    Calculates and projects monthly storage costs across all tiers.

    Usage
    -----
    model = CostModel(config)
    usage = TierUsage(...)
    breakdown = model.monthly_cost(usage)
    print(breakdown.summary())
    """

    def __init__(self, config: CostConfig | None = None):
        self.config = config or CostConfig()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def monthly_cost(self, usage: TierUsage) -> CostBreakdown:
        c = self.config
        days = 30.0

        # ---- Redis -------------------------------------------------------
        # Capacity cost (fraction of nodes needed)
        nodes_needed = math.ceil(usage.redis_used_gb / max(c.redis_usable_gb, 1e-9))
        redis_cost = nodes_needed * c.redis_instance_usd_per_month

        # ---- Postgres ----------------------------------------------------
        pg_instance = c.postgres_instance_usd_per_month
        pg_storage = usage.postgres_used_gb * c.postgres_storage_usd_per_gb_month
        # Assume ~100 IOPS/GB for an active hot tier
        pg_iops = (usage.postgres_used_gb * 100 * days * 24 * 3600 / 1e6
                   * c.postgres_iops_usd_per_million)
        postgres_cost = pg_instance + pg_storage + pg_iops

        # ---- S3 Standard (warm) -----------------------------------------
        s3_storage = usage.warm_used_gb * c.s3_standard_usd_per_gb_month
        s3_gets = (usage.warm_reads_per_day * days / 1000) * c.s3_get_usd_per_1k
        s3_puts = (usage.warm_writes_per_day * days / 1000) * c.s3_put_usd_per_1k
        warm_cost = s3_storage + s3_gets + s3_puts

        # ---- Glacier (cold) ---------------------------------------------
        glacier_storage = usage.cold_used_gb * c.s3_glacier_usd_per_gb_month

        # Average bytes per rehydration request (assume 1 MB default)
        avg_restore_gb = 0.001
        restore_rate = {
            RehydrationPriority.EXPEDITED: c.glacier_expedited_usd_per_gb,
            RehydrationPriority.STANDARD: c.glacier_standard_usd_per_gb,
            RehydrationPriority.BULK: c.glacier_bulk_usd_per_gb,
        }[usage.cold_read_priority]
        rehydration_cost = (
            usage.cold_reads_per_day * days * avg_restore_gb * restore_rate
        )
        cold_cost = glacier_storage

        # ---- Egress ------------------------------------------------------
        egress_cost = usage.egress_gb_per_day * days * c.egress_usd_per_gb

        total = redis_cost + postgres_cost + warm_cost + cold_cost + rehydration_cost + egress_cost

        return CostBreakdown(
            hot_redis_usd=redis_cost,
            hot_postgres_usd=postgres_cost,
            warm_s3_usd=warm_cost,
            cold_archive_usd=cold_cost,
            rehydration_usd=rehydration_cost,
            total_usd=total,
            details={
                "redis_nodes": nodes_needed,
                "postgres_storage_gb": usage.postgres_used_gb,
                "warm_gets_30d": usage.warm_reads_per_day * days,
                "warm_puts_30d": usage.warm_writes_per_day * days,
                "cold_restores_30d": usage.cold_reads_per_day * days,
                "egress_30d_gb": usage.egress_gb_per_day * days,
                "egress_cost_usd": egress_cost,
            },
        )

    # ------------------------------------------------------------------
    # Projection helpers
    # ------------------------------------------------------------------

    def project_from_metrics(
        self,
        hot_metrics: TierMetrics,
        warm_metrics: TierMetrics,
        cold_metrics: TierMetrics,
        hot_reads_per_day: float = 1000,
        warm_reads_per_day: float = 100,
        cold_reads_per_day: float = 10,
        rehydration_priority: RehydrationPriority = RehydrationPriority.STANDARD,
        egress_gb_per_day: float = 1.0,
    ) -> CostBreakdown:
        """Convenience wrapper that builds TierUsage from live TierMetrics."""
        usage = TierUsage(
            redis_used_gb=hot_metrics.total_size_bytes / 1e9 * 0.5,  # half in Redis
            postgres_used_gb=hot_metrics.total_size_bytes / 1e9,
            hot_reads_per_day=hot_reads_per_day,
            hot_writes_per_day=hot_reads_per_day * 0.1,
            warm_used_gb=warm_metrics.total_size_bytes / 1e9,
            warm_reads_per_day=warm_reads_per_day,
            warm_writes_per_day=warm_reads_per_day * 0.05,
            cold_used_gb=cold_metrics.total_size_bytes / 1e9,
            cold_reads_per_day=cold_reads_per_day,
            cold_read_priority=rehydration_priority,
            egress_gb_per_day=egress_gb_per_day,
        )
        return self.monthly_cost(usage)

    def savings_from_demotion(
        self,
        data_gb: float,
        from_tier: str = "hot",
        to_tier: str = "warm",
    ) -> float:
        """
        Returns estimated monthly USD savings from moving `data_gb`
        from one tier to another (storage cost delta only).
        """
        c = self.config
        rates = {
            "hot_redis": c.redis_instance_usd_per_month / max(c.redis_usable_gb, 1),
            "hot_postgres": c.postgres_storage_usd_per_gb_month,
            "warm": c.s3_standard_usd_per_gb_month,
            "cold": c.s3_glacier_usd_per_gb_month,
        }
        from_rate = rates.get(from_tier, rates["hot_postgres"])
        to_rate = rates.get(to_tier, rates["warm"])
        return data_gb * (from_rate - to_rate)

    def breakeven_days(self, data_gb: float, from_tier: str, to_tier: str) -> float:
        """
        Days until migration cost (egress + ops) is recovered in storage savings.
        Assumes one-time migration egress cost.
        """
        migration_cost = data_gb * self.config.egress_usd_per_gb
        monthly_saving = self.savings_from_demotion(data_gb, from_tier, to_tier)
        if monthly_saving <= 0:
            return float("inf")
        return migration_cost / (monthly_saving / 30)
