from __future__ import annotations
from datetime import datetime

import redis.asyncio as aioredis
import structlog

from ..config import settings
from ..models import ValidationResult, ValidationStatus

log = structlog.get_logger(__name__)

_BLOCK_PREFIX = settings.redis_block_key_prefix      # "dq:block:"
_BLOCK_TTL    = settings.block_ttl_seconds           # 3600 s


class JobController:
    """
    Controls downstream job execution based on validation outcomes.

    A Redis key ``dq:block:<job_name>`` is set when a failure is detected.
    Downstream schedulers call ``is_blocked(job_name)`` before launching.
    """

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client

    # ------------------------------------------------------------------
    # Core gate logic
    # ------------------------------------------------------------------

    async def apply_result(self, result: ValidationResult) -> list[str]:
        """
        Inspect *result* and block / unblock downstream jobs accordingly.
        Returns a list of job names that were newly blocked.
        """
        newly_blocked: list[str] = []

        if result.status in (ValidationStatus.FAILED, ValidationStatus.ERROR):
            for job in settings.downstream_jobs:
                blocked = await self._block_job(job, result)
                if blocked:
                    newly_blocked.append(job)
        else:
            # All checks passed — clear any previous blocks for this table
            await self._clear_table_blocks(result.table_name)

        return newly_blocked

    async def is_blocked(self, job_name: str) -> bool:
        """Return True if *job_name* is currently gated."""
        key = f"{_BLOCK_PREFIX}{job_name}"
        return await self._redis.exists(key) == 1

    async def list_active_blocks(self) -> list[dict]:
        """Return all active block records as a list of dicts."""
        pattern = f"{_BLOCK_PREFIX}*"
        keys = await self._redis.keys(pattern)
        blocks = []
        for key in keys:
            raw = await self._redis.hgetall(key)
            if raw:
                blocks.append(
                    {k.decode(): v.decode() for k, v in raw.items()}
                    | {"job_name": key.decode().removeprefix(_BLOCK_PREFIX)}
                )
        return blocks

    async def force_unblock(self, job_name: str) -> bool:
        """Manually lift a block (e.g. after human review). Returns True if removed."""
        key = f"{_BLOCK_PREFIX}{job_name}"
        removed = await self._redis.delete(key)
        if removed:
            log.info("block_force_lifted", job=job_name)
        return bool(removed)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _block_job(self, job_name: str, result: ValidationResult) -> bool:
        """Write a block record. Returns True if this is a *new* block."""
        key = f"{_BLOCK_PREFIX}{job_name}"
        is_new = not (await self._redis.exists(key))

        await self._redis.hset(
            key,
            mapping={
                "job_name": job_name,
                "table_name": result.table_name,
                "batch_id": result.batch_id,
                "status": result.status.value,
                "pass_rate": str(result.pass_rate),
                "failed_checks": str(result.failed_checks),
                "blocked_at": datetime.utcnow().isoformat(),
                "result_id": result.result_id,
            },
        )
        await self._redis.expire(key, _BLOCK_TTL)

        if is_new:
            log.warning(
                "job_blocked",
                job=job_name,
                table=result.table_name,
                pass_rate=result.pass_rate,
                failed_checks=result.failed_checks,
            )
        return is_new

    async def _clear_table_blocks(self, table_name: str) -> None:
        """Lift all blocks that were set because of *table_name* failures."""
        pattern = f"{_BLOCK_PREFIX}*"
        keys = await self._redis.keys(pattern)
        for key in keys:
            raw = await self._redis.hgetall(key)
            if raw and raw.get(b"table_name", b"").decode() == table_name:
                job = key.decode().removeprefix(_BLOCK_PREFIX)
                await self._redis.delete(key)
                log.info("block_lifted", job=job, table=table_name)
