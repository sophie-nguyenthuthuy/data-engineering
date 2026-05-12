"""Ingestion job: pull new emails per tenant → publish to Pub/Sub."""
from __future__ import annotations

import uuid

from .config import load
from .stubs import gmail, pubsub, warehouse


def run_once(count_per_tenant: int = 5) -> dict:
    cfg = load()
    topic = cfg["streaming"]["ingest_topic"]
    published = 0
    run_id = str(uuid.uuid4())
    warehouse.start_run(run_id, kind="ingest")
    try:
        for tenant in cfg["tenants"]:
            for email in gmail.iter_new_messages(tenant["id"], tenant["labels"], count_per_tenant):
                warehouse.insert_raw(email.to_dict())
                pubsub.publish(topic, email.to_dict(), attributes={"tenant_id": tenant["id"]})
                published += 1
        warehouse.finish_run(run_id, "ok", details=f"published={published}")
    except Exception as exc:
        warehouse.finish_run(run_id, "failed", details=str(exc))
        raise
    return {"run_id": run_id, "published": published}
