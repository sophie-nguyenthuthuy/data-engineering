"""
Transparent read router — resolves the correct tier for every GET request
without the caller needing to know where data lives.

Read path (waterfall):
  1. Hot tier  (Redis → Postgres)   — sub-ms to low-ms
  2. Warm tier (S3 Parquet)         — tens to hundreds of ms
  3. Cold tier (Glacier/archive)    — triggers async rehydration, returns
                                      a RehydrationJob + optional blocking wait

Write path:
  Always writes to hot tier.  The lifecycle engine handles demotion.

Read-through promotion:
  When a key is found on warm and promotion_threshold_freq is exceeded,
  the router automatically copies it back to hot.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from tiered_storage.schemas import (
    DataRecord,
    RehydrationJob,
    RehydrationPriority,
    Tier,
)

log = logging.getLogger(__name__)


@dataclass
class RouteResult:
    record: Optional[DataRecord]
    tier_hit: Tier
    latency_ms: float
    promoted: bool = False                     # was a read-through promotion applied?
    rehydration_job: Optional[RehydrationJob] = None  # set when cold restore queued


@dataclass
class RouterStats:
    hot_hits: int = 0
    warm_hits: int = 0
    cold_misses: int = 0   # cold rehydrations triggered
    total_misses: int = 0  # key not found anywhere
    promotions: int = 0
    total_reads: int = 0

    @property
    def hit_rate(self) -> float:
        if self.total_reads == 0:
            return 0.0
        hits = self.hot_hits + self.warm_hits
        return hits / self.total_reads

    def summary(self) -> str:
        return (
            f"Reads={self.total_reads}  "
            f"Hot={self.hot_hits}  Warm={self.warm_hits}  "
            f"Rehydrations={self.cold_misses}  "
            f"Promotions={self.promotions}  "
            f"HitRate={self.hit_rate:.1%}"
        )


class ReadRouter:
    """
    Routes reads across tiers transparently.

    Parameters
    ----------
    hot_tier              : HotTier instance
    warm_tier             : WarmTier instance
    cold_tier             : ColdTier instance
    rehydration_manager   : RehydrationManager instance
    tracker               : AccessPatternTracker instance
    promote_freq_threshold: if a warm-hit key's EMA freq exceeds this
                            (accesses/day) it is read-through promoted to hot
    block_on_cold         : if True, GET on a cold key waits for rehydration
                            to complete before returning (synchronous restore)
    default_cold_priority : rehydration priority when cold data is requested
    """

    def __init__(
        self,
        hot_tier,
        warm_tier,
        cold_tier,
        rehydration_manager,
        tracker,
        promote_freq_threshold: float = 5.0,
        block_on_cold: bool = False,
        default_cold_priority: RehydrationPriority = RehydrationPriority.STANDARD,
    ):
        self._hot = hot_tier
        self._warm = warm_tier
        self._cold = cold_tier
        self._rehydration = rehydration_manager
        self._tracker = tracker
        self._promote_threshold = promote_freq_threshold
        self._block_on_cold = block_on_cold
        self._default_cold_priority = default_cold_priority
        self.stats = RouterStats()

    # ------------------------------------------------------------------
    # Core read
    # ------------------------------------------------------------------

    async def get(
        self,
        key: str,
        cold_priority: Optional[RehydrationPriority] = None,
        block_on_cold: Optional[bool] = None,
    ) -> RouteResult:
        """
        Read `key` from whichever tier holds it, transparently.

        Returns a RouteResult.  If the key is cold and block_on_cold=False,
        result.record is None and result.rehydration_job carries the job handle.
        """
        t0 = time.perf_counter()
        self.stats.total_reads += 1

        # Track the access regardless of tier
        stats = self._tracker.record_access(key)

        # ---- 1. Hot tier ------------------------------------------------
        record = await self._hot.get(key)
        if record is not None:
            self.stats.hot_hits += 1
            return RouteResult(
                record=record,
                tier_hit=Tier.HOT,
                latency_ms=(time.perf_counter() - t0) * 1000,
            )

        # ---- 2. Warm tier -----------------------------------------------
        record = await self._warm.get(key)
        if record is not None:
            self.stats.warm_hits += 1
            promoted = False
            if stats.ema_freq >= self._promote_threshold:
                log.debug("Promoting key=%s from warm→hot (freq=%.2f)", key, stats.ema_freq)
                await self._hot.put(record)
                self.stats.promotions += 1
                promoted = True
            return RouteResult(
                record=record,
                tier_hit=Tier.WARM,
                latency_ms=(time.perf_counter() - t0) * 1000,
                promoted=promoted,
            )

        # ---- 3. Cold tier (trigger rehydration) -------------------------
        if await self._cold.exists(key):
            self.stats.cold_misses += 1
            priority = cold_priority or self._default_cold_priority
            job = self._rehydration.request_restore(key, priority=priority)

            do_block = block_on_cold if block_on_cold is not None else self._block_on_cold
            if do_block:
                record = await self._rehydration.wait_for_key(
                    key, timeout_s=job.sla_deadline - time.time() + 60
                )
                return RouteResult(
                    record=record,
                    tier_hit=Tier.COLD,
                    latency_ms=(time.perf_counter() - t0) * 1000,
                    rehydration_job=job,
                )

            return RouteResult(
                record=None,
                tier_hit=Tier.COLD,
                latency_ms=(time.perf_counter() - t0) * 1000,
                rehydration_job=job,
            )

        # ---- 4. Not found anywhere --------------------------------------
        self.stats.total_misses += 1
        return RouteResult(
            record=None,
            tier_hit=Tier.UNKNOWN,
            latency_ms=(time.perf_counter() - t0) * 1000,
        )

    # ------------------------------------------------------------------
    # Write (always hot)
    # ------------------------------------------------------------------

    async def put(self, record: DataRecord) -> None:
        """Write a record to hot tier; lifecycle engine handles promotion/demotion."""
        self._tracker.record_access(record.key)
        await self._hot.put(record)

    # ------------------------------------------------------------------
    # Locate tier without fetching data
    # ------------------------------------------------------------------

    async def locate(self, key: str) -> Tier:
        """Return which tier currently holds `key`."""
        if await self._hot.exists(key):
            return Tier.HOT
        if await self._warm.exists(key):
            return Tier.WARM
        if await self._cold.exists(key):
            return Tier.COLD
        return Tier.UNKNOWN
