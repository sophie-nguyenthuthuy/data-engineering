"""Core replay engine — orchestrates reading from S3 and writing to targets."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

import anyio
import structlog

from replay.archive.s3 import S3ArchiveReader
from replay.engine.checkpoint import CheckpointStore
from replay.models import ReplayConfig, ReplayProgress, ReplayStatus
from replay.targets.base import BaseTarget

logger = structlog.get_logger(__name__)


class ReplayEngine:
    """
    Orchestrates the full replay lifecycle:
      1. List S3 files for each topic in the time window
      2. Stream events through the target adapter (with rate limiting)
      3. Checkpoint progress so the job can be resumed after failure
      4. Emit progress updates via an async queue
    """

    def __init__(self, config: ReplayConfig, target: BaseTarget) -> None:
        self.config = config
        self.target = target
        self._reader = S3ArchiveReader(config.archive)
        self._checkpoint = CheckpointStore(config.checkpoint_dir, config.job_id)
        self._progress = ReplayProgress(
            job_id=config.job_id,
            replayed_events=self._checkpoint.get_replayed_count(),
            failed_events=self._checkpoint.get_failed_count(),
        )
        self._stop_event = asyncio.Event()
        self.progress_queue: asyncio.Queue[ReplayProgress] = asyncio.Queue(maxsize=128)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> ReplayProgress:
        """Execute the replay job end-to-end. Returns final progress."""
        self._progress.status = ReplayStatus.RUNNING
        self._progress.started_at = datetime.now(tz=timezone.utc)
        await self._emit_progress()

        try:
            await self.target.open()
            await self._run_replay()
            self._progress.status = ReplayStatus.COMPLETED
        except asyncio.CancelledError:
            self._progress.status = ReplayStatus.PAUSED
            logger.info("replay_paused", job_id=self.config.job_id)
            raise
        except Exception as exc:
            self._progress.status = ReplayStatus.FAILED
            self._progress.errors.append(str(exc))
            logger.exception("replay_failed", job_id=self.config.job_id, error=str(exc))
            raise
        finally:
            self._progress.completed_at = datetime.now(tz=timezone.utc)
            self._checkpoint.record_progress(
                self._progress.replayed_events,
                self._progress.failed_events,
            )
            await self.target.close()
            await self._emit_progress()

        logger.info(
            "replay_complete",
            job_id=self.config.job_id,
            replayed=self._progress.replayed_events,
            failed=self._progress.failed_events,
            skipped=self._progress.skipped_events,
        )
        return self._progress

    def stop(self) -> None:
        """Signal the engine to stop gracefully after the current file."""
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _run_replay(self) -> None:
        cfg = self.config
        sem = asyncio.Semaphore(cfg.max_parallel_partitions)

        # Gather all S3 keys across topics
        all_keys: list[tuple[str, str]] = []  # (topic, key)
        for topic in cfg.topics:
            keys = await self._reader.list_files(topic, cfg.window)
            for key in keys:
                all_keys.append((topic, key))

        self._progress.total_events = len(all_keys)  # rough estimate; updated per file
        await self._emit_progress()

        # Build rate limiter token bucket
        rate_limiter = _TokenBucket(cfg.rate_limit_per_second) if cfg.rate_limit_per_second else None

        async def process_key(topic: str, key: str) -> None:
            async with sem:
                if self._stop_event.is_set():
                    return
                if self._checkpoint.is_key_done(key):
                    self._progress.skipped_events += 1
                    logger.debug("checkpoint_skip", key=key)
                    return

                log = logger.bind(topic=topic, key=key)
                log.info("processing_file")

                events_in_file = 0
                async for event in self._reader.read_events(key, cfg.window):
                    if self._stop_event.is_set():
                        break
                    if rate_limiter:
                        await rate_limiter.acquire()

                    if cfg.dry_run:
                        logger.debug("dry_run_event", event_id=event.event_id, ts=event.timestamp)
                        self._progress.replayed_events += 1
                    else:
                        try:
                            await self.target.send(event)
                            self._progress.replayed_events += 1
                        except Exception as exc:
                            self._progress.failed_events += 1
                            logger.warning("send_error", error=str(exc), event_id=event.event_id)
                    events_in_file += 1

                if events_in_file > 0:
                    self._checkpoint.mark_key_done(key)
                    self._progress.last_checkpoint = datetime.now(tz=timezone.utc)

                self._checkpoint.record_progress(
                    self._progress.replayed_events,
                    self._progress.failed_events,
                )
                await self._emit_progress()

        tasks = [process_key(topic, key) for topic, key in all_keys]
        await asyncio.gather(*tasks)

    async def _emit_progress(self) -> None:
        try:
            self.progress_queue.put_nowait(self._progress.model_copy())
        except asyncio.QueueFull:
            pass  # non-blocking; consumer may be slow


class _TokenBucket:
    """Simple token-bucket rate limiter."""

    def __init__(self, rate: float) -> None:
        self._rate = rate          # tokens per second
        self._tokens = rate
        self._last = time.monotonic()

    async def acquire(self) -> None:
        while True:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens >= 1:
                self._tokens -= 1
                return
            wait = (1 - self._tokens) / self._rate
            await asyncio.sleep(wait)
