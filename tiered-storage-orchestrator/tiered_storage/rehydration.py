"""
Rehydration manager — restores cold data to warm (or hot) tier with SLA guarantees.

SLA windows (configurable, defaults match AWS Glacier):
  - EXPEDITED : 5 minutes
  - STANDARD  : 5 hours
  - BULK      : 12 hours

The manager:
  1. Accepts restore requests and returns a RehydrationJob with a deadline.
  2. Executes the restore (cold → warm copy) asynchronously.
  3. Tracks SLA compliance and exposes metrics.
  4. Optionally auto-promotes hot keys (warm → hot) after restore.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Callable, Optional

from tiered_storage.schemas import (
    REHYDRATION_SLA_SECONDS,
    DataRecord,
    RehydrationJob,
    RehydrationPriority,
    Tier,
)

log = logging.getLogger(__name__)


class SLAViolation(Exception):
    """Raised when a rehydration job completes past its SLA deadline."""


class RehydrationManager:
    """
    Manages restore jobs from cold → warm (and optionally warm → hot).

    Parameters
    ----------
    cold_tier    : ColdTier instance
    warm_tier    : WarmTier instance
    hot_tier     : optional HotTier; if supplied, expedited restores land on hot
    on_complete  : optional async callback(job: RehydrationJob, record: DataRecord)
    """

    def __init__(
        self,
        cold_tier,
        warm_tier,
        hot_tier=None,
        on_complete: Optional[Callable] = None,
    ):
        self._cold = cold_tier
        self._warm = warm_tier
        self._hot = hot_tier
        self._on_complete = on_complete

        self._jobs: dict[str, RehydrationJob] = {}
        self._pending: asyncio.Queue = asyncio.Queue()

        # SLA metrics
        self._sla_met: int = 0
        self._sla_violated: int = 0
        self._total_latency_s: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def request_restore(
        self,
        key: str,
        priority: RehydrationPriority = RehydrationPriority.STANDARD,
        target_tier: Tier = Tier.WARM,
    ) -> RehydrationJob:
        """
        Enqueue a restore request.  Returns the job immediately so callers
        can poll job.completed_at or await wait_for_key().
        """
        # Deduplicate: return existing job if already in-flight
        for job in self._jobs.values():
            if job.key == key and job.completed_at is None:
                log.debug("Rehydration already in-flight for key=%s", key)
                return job

        sla_seconds = REHYDRATION_SLA_SECONDS[priority]
        now = time.time()
        job = RehydrationJob(
            job_id=str(uuid.uuid4()),
            key=key,
            priority=priority,
            requested_at=now,
            sla_deadline=now + sla_seconds,
            target_tier=target_tier,
        )
        self._jobs[job.job_id] = job
        self._pending.put_nowait(job)
        log.info(
            "Restore requested key=%s priority=%s SLA=%.0fs job_id=%s",
            key, priority.value, sla_seconds, job.job_id,
        )
        return job

    async def wait_for_key(
        self,
        key: str,
        timeout_s: Optional[float] = None,
    ) -> Optional[DataRecord]:
        """
        Block until a pending restore for `key` completes or timeout elapses.
        Returns the DataRecord on success, None on timeout.
        """
        deadline = time.time() + (timeout_s or 3600)
        while time.time() < deadline:
            # Check if any completed job matches
            for job in self._jobs.values():
                if job.key == key and job.completed_at is not None:
                    return await self._warm.get(key) or (
                        await self._hot.get(key) if self._hot else None
                    )
            await asyncio.sleep(0.1)
        return None

    async def process_queue(self, max_concurrent: int = 4) -> None:
        """
        Drain the restore queue.  Run this in a background task:
            asyncio.create_task(manager.process_queue())
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def _worker(job: RehydrationJob) -> None:
            async with semaphore:
                await self._execute_restore(job)

        while True:
            job = await self._pending.get()
            asyncio.create_task(_worker(job))
            self._pending.task_done()

    async def run_once(self) -> int:
        """Drain all currently queued jobs synchronously.  Useful in tests."""
        processed = 0
        while not self._pending.empty():
            job = self._pending.get_nowait()
            await self._execute_restore(job)
            self._pending.task_done()
            processed += 1
        return processed

    # ------------------------------------------------------------------
    # SLA metrics
    # ------------------------------------------------------------------

    @property
    def sla_compliance_rate(self) -> float:
        total = self._sla_met + self._sla_violated
        return self._sla_met / total if total else 1.0

    @property
    def avg_restore_latency_s(self) -> float:
        total = self._sla_met + self._sla_violated
        return self._total_latency_s / total if total else 0.0

    def sla_report(self) -> dict:
        total = self._sla_met + self._sla_violated
        return {
            "total_jobs": total,
            "sla_met": self._sla_met,
            "sla_violated": self._sla_violated,
            "compliance_rate_pct": round(self.sla_compliance_rate * 100, 2),
            "avg_restore_latency_s": round(self.avg_restore_latency_s, 2),
            "pending_jobs": self._pending.qsize(),
            "in_flight": sum(1 for j in self._jobs.values() if j.completed_at is None),
        }

    def get_job(self, job_id: str) -> Optional[RehydrationJob]:
        return self._jobs.get(job_id)

    def list_jobs(self, completed: Optional[bool] = None) -> list[RehydrationJob]:
        jobs = list(self._jobs.values())
        if completed is True:
            jobs = [j for j in jobs if j.completed_at is not None]
        elif completed is False:
            jobs = [j for j in jobs if j.completed_at is None]
        return sorted(jobs, key=lambda j: j.requested_at)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _execute_restore(self, job: RehydrationJob) -> None:
        start = time.time()
        try:
            record = await self._cold.get(job.key)
            if record is None:
                log.warning("Restore failed: key=%s not found in cold tier", job.key)
                job.completed_at = time.time()
                return

            # Simulate realistic Glacier restore latency in non-test mode
            # (In production the S3 restore_object() call governs timing;
            #  here we add a small artificial delay for expedited vs standard.)
            await self._simulate_restore_delay(job.priority)

            # Write to target tier
            record.tier = job.target_tier
            if job.target_tier == Tier.HOT and self._hot:
                await self._hot.put(record)
            else:
                await self._warm.put(record)

            job.completed_at = time.time()
            latency = job.completed_at - start
            self._total_latency_s += latency

            if job.sla_met:
                self._sla_met += 1
                log.info(
                    "Restore COMPLETE (SLA MET) key=%s latency=%.1fs",
                    job.key, latency,
                )
            else:
                self._sla_violated += 1
                log.warning(
                    "Restore COMPLETE (SLA VIOLATED) key=%s latency=%.1fs deadline=%.1fs overdue",
                    job.key, latency, job.completed_at - job.sla_deadline,
                )

            if self._on_complete:
                await self._on_complete(job, record)

        except Exception as exc:
            log.exception("Restore error for key=%s: %s", job.key, exc)
            job.completed_at = time.time()

    @staticmethod
    async def _simulate_restore_delay(priority: RehydrationPriority) -> None:
        """
        Tiny synthetic delay so async tests can observe ordering.
        In production replace with actual S3 restore_object polling.
        """
        delays = {
            RehydrationPriority.EXPEDITED: 0.01,
            RehydrationPriority.STANDARD: 0.05,
            RehydrationPriority.BULK: 0.1,
        }
        await asyncio.sleep(delays[priority])
