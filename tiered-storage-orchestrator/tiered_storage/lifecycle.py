"""
Lifecycle policy engine — scans tiers on a schedule and moves data
between hot → warm → cold based on access patterns and size caps.

Demotion rules (evaluated in order):
  1. Hot → Warm  if idle > hot_to_warm_idle_days
                 OR ema_freq < hot_min_access_freq
                 OR hot tier exceeds hot_max_size_gb
  2. Warm → Cold if idle > warm_to_cold_idle_days
                 OR ema_freq < warm_min_access_freq
                 OR warm tier exceeds warm_max_size_gb

Promotion rules:
  • Warm → Hot   if ema_freq >= hot_min_access_freq * 2  (manual trigger only)
  • Cold → Warm  handled by RehydrationManager on demand
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from tiered_storage.schemas import DataRecord, LifecyclePolicy, Tier

log = logging.getLogger(__name__)


@dataclass
class MigrationRecord:
    key: str
    from_tier: Tier
    to_tier: Tier
    reason: str
    size_bytes: int
    timestamp: float = field(default_factory=time.time)


@dataclass
class CycleReport:
    cycle_at: float
    hot_to_warm: list[MigrationRecord] = field(default_factory=list)
    warm_to_cold: list[MigrationRecord] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def total_demotions(self) -> int:
        return len(self.hot_to_warm) + len(self.warm_to_cold)

    @property
    def bytes_demoted(self) -> int:
        return sum(m.size_bytes for m in self.hot_to_warm + self.warm_to_cold)

    def summary(self) -> str:
        return (
            f"Cycle at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.cycle_at))} | "
            f"hot→warm={len(self.hot_to_warm)} warm→cold={len(self.warm_to_cold)} "
            f"bytes_moved={self.bytes_demoted:,} errors={len(self.errors)}"
        )


class LifecycleEngine:
    """
    Evaluates tier-transition rules and executes migrations.

    Typical usage (background task):
        engine = LifecycleEngine(policy, hot, warm, cold, tracker)
        asyncio.create_task(engine.run_forever(interval_seconds=3600))

    Or single-shot (e.g. in tests):
        report = await engine.run_cycle()
    """

    def __init__(
        self,
        policy: LifecyclePolicy,
        hot_tier,
        warm_tier,
        cold_tier,
        tracker,
    ):
        self._policy = policy
        self._hot = hot_tier
        self._warm = warm_tier
        self._cold = cold_tier
        self._tracker = tracker
        self._history: list[CycleReport] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run_forever(self, interval_seconds: float = 3600) -> None:
        """Background loop — run as an asyncio task."""
        log.info("Lifecycle engine started (interval=%ds)", interval_seconds)
        while True:
            try:
                report = await self.run_cycle()
                log.info(report.summary())
            except Exception as exc:
                log.exception("Lifecycle cycle failed: %s", exc)
            await asyncio.sleep(interval_seconds)

    async def run_cycle(self) -> CycleReport:
        """Execute one full scan of hot and warm tiers."""
        report = CycleReport(cycle_at=time.time())

        await self._demote_hot_to_warm(report)
        await self._demote_warm_to_cold(report)

        self._history.append(report)
        # Keep last 100 cycle reports
        if len(self._history) > 100:
            self._history.pop(0)

        return report

    def last_cycle(self) -> CycleReport | None:
        return self._history[-1] if self._history else None

    def history(self) -> list[CycleReport]:
        return list(self._history)

    # ------------------------------------------------------------------
    # Demotion logic
    # ------------------------------------------------------------------

    async def _demote_hot_to_warm(self, report: CycleReport) -> None:
        p = self._policy
        try:
            candidates = await self._hot.get_stale_keys(
                idle_days=p.hot_to_warm_idle_days,
                min_freq=p.hot_min_access_freq,
            )
        except Exception as exc:
            report.errors.append(f"hot.get_stale_keys: {exc}")
            return

        # Also enforce size cap
        try:
            hot_metrics = await self._hot.metrics()
            hot_size_gb = hot_metrics.total_size_bytes / 1e9
            if hot_size_gb > p.hot_max_size_gb:
                extra = await self._hot.list_keys(limit=500)
                candidates = list(dict.fromkeys(candidates + extra))  # dedup
                log.warning(
                    "Hot tier over cap (%.2f GB > %.2f GB) — forcing extra demotions",
                    hot_size_gb, p.hot_max_size_gb,
                )
        except Exception as exc:
            report.errors.append(f"hot.metrics: {exc}")

        for key in candidates:
            try:
                record = await self._hot.get(key)
                if record is None:
                    continue
                reason = self._hot_demotion_reason(record, p)
                record.tier = Tier.WARM
                await self._warm.put(record)
                await self._hot.delete(key)
                self._tracker.remove(key)  # reset EMA so warm tracker starts fresh
                migration = MigrationRecord(
                    key=key,
                    from_tier=Tier.HOT,
                    to_tier=Tier.WARM,
                    reason=reason,
                    size_bytes=record.size_bytes,
                )
                report.hot_to_warm.append(migration)
                log.debug("Demoted hot→warm key=%s reason=%s", key, reason)
            except Exception as exc:
                report.errors.append(f"hot→warm key={key}: {exc}")

    async def _demote_warm_to_cold(self, report: CycleReport) -> None:
        p = self._policy
        try:
            candidates = await self._warm.get_stale_keys(
                idle_days=p.warm_to_cold_idle_days,
                min_freq=p.warm_min_access_freq,
            )
        except Exception as exc:
            report.errors.append(f"warm.get_stale_keys: {exc}")
            return

        # Size cap enforcement
        try:
            warm_metrics = await self._warm.metrics()
            warm_size_gb = warm_metrics.total_size_bytes / 1e9
            if warm_size_gb > p.warm_max_size_gb:
                extra = await self._warm.list_keys(limit=500)
                candidates = list(dict.fromkeys(candidates + extra))
                log.warning(
                    "Warm tier over cap (%.2f GB > %.2f GB) — forcing extra demotions",
                    warm_size_gb, p.warm_max_size_gb,
                )
        except Exception as exc:
            report.errors.append(f"warm.metrics: {exc}")

        for key in candidates:
            try:
                record = await self._warm.get(key)
                if record is None:
                    continue
                reason = self._warm_demotion_reason(record, p)
                record.tier = Tier.COLD
                await self._cold.put(record)
                await self._warm.delete(key)
                migration = MigrationRecord(
                    key=key,
                    from_tier=Tier.WARM,
                    to_tier=Tier.COLD,
                    reason=reason,
                    size_bytes=record.size_bytes,
                )
                report.warm_to_cold.append(migration)
                log.debug("Demoted warm→cold key=%s reason=%s", key, reason)
            except Exception as exc:
                report.errors.append(f"warm→cold key={key}: {exc}")

    # ------------------------------------------------------------------
    # Reason helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _hot_demotion_reason(record: DataRecord, p: LifecyclePolicy) -> str:
        idle = (time.time() - record.last_accessed_at) / 86400
        if idle >= p.hot_to_warm_idle_days:
            return f"idle_{idle:.1f}d"
        return "low_freq"

    @staticmethod
    def _warm_demotion_reason(record: DataRecord, p: LifecyclePolicy) -> str:
        idle = (time.time() - record.last_accessed_at) / 86400
        if idle >= p.warm_to_cold_idle_days:
            return f"idle_{idle:.1f}d"
        return "low_freq"
