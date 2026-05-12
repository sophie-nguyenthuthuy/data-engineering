"""
Background job: export a dataset to object storage as NDJSON.
Respects concurrent job quotas via Redis counter.
"""
import asyncio
import json
from uuid import UUID

import structlog

from workers.celery_app import celery_app
from core.config import settings
from core.quotas.engine import QuotaEngine
from core.storage.object_store import ObjectStore
from db.session import get_tenant_session
from db.models.data import DataRecord, Dataset
from sqlalchemy import select

import redis


log = structlog.get_logger()


@celery_app.task(bind=True, max_retries=3, default_retry_delay=30)
def export_dataset(self, tenant_id: str, dataset_id: str, tier: str = "free") -> dict:
    return asyncio.get_event_loop().run_until_complete(
        _export_dataset_async(self, UUID(tenant_id), UUID(dataset_id), tier)
    )


async def _export_dataset_async(task, tenant_id: UUID, dataset_id: UUID, tier: str) -> dict:
    redis_client = redis.asyncio.from_url(settings.redis_url)
    quota = QuotaEngine(redis_client)

    job_result = await quota.check_job(tenant_id, tier)
    if not job_result.allowed:
        raise task.retry(exc=Exception("Concurrent job limit reached"))

    try:
        store = ObjectStore()
        rows: list[str] = []

        async with get_tenant_session(tenant_id) as session:
            result = await session.execute(
                select(DataRecord).where(
                    DataRecord.dataset_id == dataset_id,
                    DataRecord.tenant_id == tenant_id,
                )
            )
            for record in result.scalars():
                rows.append(json.dumps({"id": str(record.id), **record.data}))

        export_key = f"exports/{dataset_id}/latest.ndjson"
        payload = "\n".join(rows).encode()
        obj = await store.put_object(
            tenant_id=tenant_id,
            key=export_key,
            data=payload,
            content_type="application/x-ndjson",
        )

        log.info("export.complete", tenant_id=str(tenant_id), dataset_id=str(dataset_id), rows=len(rows))
        return {"key": export_key, "rows": len(rows), "size_bytes": obj.size}
    finally:
        await quota.release_job(tenant_id)
