"""
Periodic task: sync storage usage from DB to Redis cache and Tenant table.
Run every 15 minutes via Celery Beat.
"""
import asyncio
from sqlalchemy import select, func

from workers.celery_app import celery_app
from core.config import settings
from core.quotas.engine import QuotaEngine
from db.session import get_admin_session
from db.models.data import Dataset
from db.models.tenant import Tenant

import redis as sync_redis


@celery_app.task
def sync_storage_quotas() -> dict:
    return asyncio.get_event_loop().run_until_complete(_sync())


async def _sync() -> dict:
    redis_client = sync_redis.asyncio.from_url(settings.redis_url)
    quota = QuotaEngine(redis_client)
    synced = 0

    async with get_admin_session() as session:
        result = await session.execute(
            select(Dataset.tenant_id, func.sum(Dataset.size_bytes))
            .group_by(Dataset.tenant_id)
        )
        for tenant_id, total_bytes in result:
            total = int(total_bytes or 0)
            await redis_client.set(f"storage:{tenant_id}:bytes", total)
            await session.execute(
                Tenant.__table__.update()
                .where(Tenant.id == tenant_id)
                .values(storage_bytes_used=total)
            )
            synced += 1

    return {"synced_tenants": synced}
