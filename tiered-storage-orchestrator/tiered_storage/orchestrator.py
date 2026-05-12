"""
TieredStorageOrchestrator — the single entry point for all storage operations.

Wires together:
  HotTier → WarmTier → ColdTier
  AccessPatternTracker
  LifecycleEngine  (background task)
  RehydrationManager (background task)
  ReadRouter
  CostModel

Quick start
-----------
    from tiered_storage import TieredStorageOrchestrator, StorageConfig

    cfg = StorageConfig()
    orch = TieredStorageOrchestrator(cfg)
    await orch.start()

    await orch.put("user:42", {"name": "Alice", "score": 99})
    result = await orch.get("user:42")
    print(result.record)

    report = await orch.run_lifecycle_cycle()
    cost   = orch.cost_report()
    print(cost.summary())

    await orch.stop()
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from tiered_storage.config import StorageConfig
from tiered_storage.cost_model import CostModel, CostConfig, TierUsage
from tiered_storage.lifecycle import CycleReport, LifecycleEngine
from tiered_storage.rehydration import RehydrationManager
from tiered_storage.router import ReadRouter, RouteResult
from tiered_storage.schemas import (
    CostBreakdown,
    DataRecord,
    LifecyclePolicy,
    RehydrationJob,
    RehydrationPriority,
    Tier,
)
from tiered_storage.tiers.cold import ColdTier
from tiered_storage.tiers.hot import HotTier
from tiered_storage.tiers.warm import WarmTier
from tiered_storage.tracking.access_patterns import AccessPatternTracker

log = logging.getLogger(__name__)


class TieredStorageOrchestrator:
    """
    High-level facade over the entire tiered storage stack.

    All public methods are async and safe to call from any coroutine.
    Call ``await start()`` before use and ``await stop()`` on shutdown.
    """

    def __init__(
        self,
        config: Optional[StorageConfig] = None,
        cost_config: Optional[CostConfig] = None,
        # Inject pre-built tiers (useful for testing with mock/local backends)
        hot_tier: Optional[HotTier] = None,
        warm_tier: Optional[WarmTier] = None,
        cold_tier: Optional[ColdTier] = None,
    ):
        self._cfg = config or StorageConfig()
        self._cost_cfg = cost_config or CostConfig()
        self._started = False
        self._bg_tasks: list[asyncio.Task] = []

        # ---- Tiers -------------------------------------------------------
        self.hot: HotTier = hot_tier or HotTier(
            redis_url=self._cfg.redis_url,
            postgres_dsn=self._cfg.postgres_dsn,
            redis_ttl_seconds=self._cfg.redis_ttl_seconds,
        )
        self.warm: WarmTier = warm_tier or WarmTier(
            bucket=self._cfg.s3_bucket,
            prefix=self._cfg.s3_warm_prefix,
            region=self._cfg.s3_region,
            aws_access_key_id=self._cfg.aws_access_key_id,
            aws_secret_access_key=self._cfg.aws_secret_access_key,
            endpoint_url=self._cfg.s3_endpoint_url,
        )
        self.cold: ColdTier = cold_tier or ColdTier(
            bucket=self._cfg.s3_bucket if not self._cfg.cold_local_path else None,
            local_path=self._cfg.cold_local_path,
            prefix=self._cfg.s3_cold_prefix,
            region=self._cfg.s3_region,
            endpoint_url=self._cfg.s3_endpoint_url,
            use_glacier=self._cfg.use_glacier,
        )

        # ---- Access tracking ---------------------------------------------
        self.tracker = AccessPatternTracker(
            persist_path=self._cfg.tracker_persist_path
        )

        # ---- Lifecycle & rehydration -------------------------------------
        policy: LifecyclePolicy = self._cfg.to_lifecycle_policy()

        self.rehydration = RehydrationManager(
            cold_tier=self.cold,
            warm_tier=self.warm,
            hot_tier=self.hot,
        )
        self.lifecycle = LifecycleEngine(
            policy=policy,
            hot_tier=self.hot,
            warm_tier=self.warm,
            cold_tier=self.cold,
            tracker=self.tracker,
        )

        # ---- Router ------------------------------------------------------
        self.router = ReadRouter(
            hot_tier=self.hot,
            warm_tier=self.warm,
            cold_tier=self.cold,
            rehydration_manager=self.rehydration,
            tracker=self.tracker,
            promote_freq_threshold=self._cfg.promote_freq_threshold,
            block_on_cold=self._cfg.block_on_cold,
            default_cold_priority=self._cfg.rehydration_default_priority,
        )

        # ---- Cost model --------------------------------------------------
        self._cost_model = CostModel(self._cost_cfg)

    # ------------------------------------------------------------------
    # Lifecycle management
    # ------------------------------------------------------------------

    async def start(self, run_lifecycle: bool = True) -> None:
        """Connect to all backends and (optionally) start background tasks."""
        if self._started:
            return

        log.info("Starting TieredStorageOrchestrator …")

        # Connect hot tier (Redis + Postgres)
        try:
            await self.hot.connect()
            log.info("  ✓ Hot tier connected")
        except Exception as exc:
            log.warning("  ✗ Hot tier unavailable (%s) — running degraded", exc)

        # Connect warm tier (S3)
        try:
            self.warm.connect()
            log.info("  ✓ Warm tier connected")
        except Exception as exc:
            log.warning("  ✗ Warm tier unavailable (%s) — running degraded", exc)

        # Connect cold tier (local or S3)
        try:
            self.cold.connect()
            log.info("  ✓ Cold tier connected")
        except Exception as exc:
            log.warning("  ✗ Cold tier unavailable (%s) — running degraded", exc)

        if run_lifecycle:
            lc_task = asyncio.create_task(
                self.lifecycle.run_forever(self._cfg.lifecycle_interval_seconds),
                name="lifecycle-engine",
            )
            rh_task = asyncio.create_task(
                self.rehydration.process_queue(),
                name="rehydration-queue",
            )
            self._bg_tasks = [lc_task, rh_task]

        self._started = True
        log.info("TieredStorageOrchestrator ready.")

    async def stop(self) -> None:
        """Cancel background tasks, persist tracker state, close connections."""
        for task in self._bg_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.tracker.save()

        await self.hot.close()
        log.info("TieredStorageOrchestrator stopped.")

    # ------------------------------------------------------------------
    # Core KV operations  (thin delegation to the router)
    # ------------------------------------------------------------------

    async def get(
        self,
        key: str,
        cold_priority: Optional[RehydrationPriority] = None,
        block_on_cold: Optional[bool] = None,
    ) -> RouteResult:
        """
        Read a key from whichever tier holds it.

        Returns a RouteResult with:
          .record           — DataRecord or None (if cold and not yet restored)
          .tier_hit         — which tier answered (HOT / WARM / COLD / UNKNOWN)
          .latency_ms       — end-to-end read latency
          .rehydration_job  — RehydrationJob if cold restore was triggered
        """
        return await self.router.get(
            key,
            cold_priority=cold_priority,
            block_on_cold=block_on_cold,
        )

    async def put(
        self,
        key: str,
        value: Any,
        size_bytes: Optional[int] = None,
        metadata: Optional[dict] = None,
    ) -> DataRecord:
        """Write a value to hot tier. Returns the stored DataRecord."""
        if size_bytes is None:
            import sys
            size_bytes = sys.getsizeof(value)

        record = DataRecord(
            key=key,
            value=value,
            size_bytes=size_bytes,
            metadata=metadata or {},
        )
        await self.router.put(record)
        return record

    async def delete(self, key: str) -> bool:
        """Remove a key from all tiers. Returns True if found anywhere."""
        results = await asyncio.gather(
            self.hot.delete(key),
            self.warm.delete(key),
            self.cold.delete(key),
        )
        self.tracker.remove(key)
        return any(results)

    async def locate(self, key: str) -> Tier:
        """Return which tier currently holds the key."""
        return await self.router.locate(key)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run_lifecycle_cycle(self) -> CycleReport:
        """Manually trigger one lifecycle scan (useful for testing / CLI)."""
        return await self.lifecycle.run_cycle()

    async def rehydrate(
        self,
        key: str,
        priority: RehydrationPriority = RehydrationPriority.STANDARD,
        block: bool = False,
    ) -> RehydrationJob:
        """Manually trigger rehydration for a cold key."""
        job = self.rehydration.request_restore(key, priority=priority)
        if block:
            await self.rehydration.wait_for_key(key, timeout_s=job.sla_deadline - time.time() + 60)
        return job

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    async def metrics(self) -> dict:
        """Aggregate metrics from all tiers + router + rehydration."""
        hot_m = await self.hot.metrics()
        warm_m = await self.warm.metrics()
        cold_m = await self.cold.metrics()

        return {
            "tiers": {
                "hot": {
                    "records": hot_m.record_count,
                    "size_gb": round(hot_m.total_size_bytes / 1e9, 4),
                    "avg_freq_per_day": round(hot_m.avg_access_frequency, 4),
                    "oldest_record_days": round(hot_m.oldest_record_age_days, 2),
                },
                "warm": {
                    "records": warm_m.record_count,
                    "size_gb": round(warm_m.total_size_bytes / 1e9, 4),
                    "avg_freq_per_day": round(warm_m.avg_access_frequency, 4),
                    "oldest_record_days": round(warm_m.oldest_record_age_days, 2),
                },
                "cold": {
                    "records": cold_m.record_count,
                    "size_gb": round(cold_m.total_size_bytes / 1e9, 4),
                    "avg_freq_per_day": round(cold_m.avg_access_frequency, 4),
                    "oldest_record_days": round(cold_m.oldest_record_age_days, 2),
                },
            },
            "router": {
                "total_reads": self.router.stats.total_reads,
                "hot_hits": self.router.stats.hot_hits,
                "warm_hits": self.router.stats.warm_hits,
                "cold_triggers": self.router.stats.cold_misses,
                "promotions": self.router.stats.promotions,
                "hit_rate_pct": round(self.router.stats.hit_rate * 100, 2),
            },
            "rehydration": self.rehydration.sla_report(),
            "lifecycle": {
                "last_cycle": (
                    self.lifecycle.last_cycle().summary()
                    if self.lifecycle.last_cycle()
                    else "no cycles run"
                ),
                "total_cycles": len(self.lifecycle.history()),
            },
        }

    async def cost_report(
        self,
        hot_reads_per_day: float = 1000,
        warm_reads_per_day: float = 100,
        cold_reads_per_day: float = 10,
        egress_gb_per_day: float = 1.0,
    ) -> CostBreakdown:
        """Compute monthly cost estimate from live tier metrics."""
        hot_m  = await self.hot.metrics()
        warm_m = await self.warm.metrics()
        cold_m = await self.cold.metrics()

        return self._cost_model.project_from_metrics(
            hot_metrics=hot_m,
            warm_metrics=warm_m,
            cold_metrics=cold_m,
            hot_reads_per_day=hot_reads_per_day,
            warm_reads_per_day=warm_reads_per_day,
            cold_reads_per_day=cold_reads_per_day,
            rehydration_priority=self._cfg.rehydration_default_priority,
            egress_gb_per_day=egress_gb_per_day,
        )

    async def savings_report(self) -> dict:
        """Show potential monthly savings if all warm/cold data stayed on hot."""
        hot_m  = await self.hot.metrics()
        warm_m = await self.warm.metrics()
        cold_m = await self.cold.metrics()

        warm_gb = warm_m.total_size_bytes / 1e9
        cold_gb = cold_m.total_size_bytes / 1e9

        return {
            "warm_vs_hot_savings_usd": round(
                self._cost_model.savings_from_demotion(warm_gb, "hot_postgres", "warm"), 2
            ),
            "cold_vs_warm_savings_usd": round(
                self._cost_model.savings_from_demotion(cold_gb, "warm", "cold"), 2
            ),
            "warm_breakeven_days": round(
                self._cost_model.breakeven_days(warm_gb, "hot_postgres", "warm"), 1
            ),
            "cold_breakeven_days": round(
                self._cost_model.breakeven_days(cold_gb, "warm", "cold"), 1
            ),
            "warm_data_gb": round(warm_gb, 4),
            "cold_data_gb": round(cold_gb, 4),
        }
