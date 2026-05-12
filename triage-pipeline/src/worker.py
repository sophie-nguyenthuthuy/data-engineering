"""Subscription worker: pull from ingest topic → Claude classify → warehouse
→ Slack. Exponential backoff on transient errors, DLQ after max_retries.
"""
from __future__ import annotations

import time
import uuid

from .config import load, tenant as tenant_cfg
from .stubs import llm, pubsub, slack, warehouse


def _post_slack(tenant_id: str, channel: str, email_id: str, subject: str, cls: llm.Classification) -> None:
    icon = {"high": ":rotating_light:", "med": ":warning:", "low": ":mailbox:"}[cls.priority]
    slack.post(
        tenant_id=tenant_id,
        channel=channel,
        text=f"{icon} [{cls.predicted_label}] {subject} — {cls.summary} (conf={cls.confidence:.2f})",
        blocks=[{"email_id": email_id, "priority": cls.priority, "label": cls.predicted_label}],
    )


def process_one(msg: pubsub.Message, max_retries: int, dlq_topic: str) -> str:
    """Return 'ok' | 'retry' | 'dlq'."""
    try:
        payload = msg.payload
        tenant_id = msg.attributes.get("tenant_id") or payload["tenant_id"]
        tc = tenant_cfg(tenant_id)
        cls = llm.classify(payload["subject"], payload["body"], tc["labels"])
        warehouse.insert_processed({
            "id": payload["id"],
            "tenant_id": tenant_id,
            "predicted_label": cls.predicted_label,
            "confidence": cls.confidence,
            "summary": cls.summary,
            "priority": cls.priority,
            "model": cls.model,
            "latency_ms": cls.latency_ms,
        })
        _post_slack(tenant_id, tc["slack_channel"], payload["id"], payload["subject"], cls)
        pubsub.ack(msg.message_id)
        return "ok"
    except Exception as exc:
        routed = pubsub.nack(msg.message_id, error=repr(exc), max_retries=max_retries, dlq_topic=dlq_topic)
        return "dlq" if routed else "retry"


def drain(max_iters: int = 1000) -> dict:
    """Pull until the ingest topic is empty (or max_iters). Used by smoke test
    and the orchestrator's process step."""
    cfg = load()
    topic = cfg["streaming"]["ingest_topic"]
    dlq = cfg["streaming"]["dlq_topic"]
    max_retries = cfg["processing"]["max_retries"]
    backoff_base = cfg["processing"]["backoff_base_seconds"]
    batch = cfg["processing"]["batch_size"]
    lease = cfg["streaming"]["ack_deadline_seconds"]

    run_id = str(uuid.uuid4())
    warehouse.start_run(run_id, kind="process")
    counts = {"ok": 0, "retry": 0, "dlq": 0}
    try:
        for i in range(max_iters):
            msgs = pubsub.pull(topic, max_messages=batch, lease_seconds=lease)
            if not msgs:
                break
            for m in msgs:
                outcome = process_one(m, max_retries=max_retries, dlq_topic=dlq)
                counts[outcome] += 1
                if outcome == "retry":
                    # exponential backoff within the batch
                    time.sleep(backoff_base * (2 ** min(m.delivery_count - 1, 4)))
        warehouse.finish_run(run_id, "ok", details=str(counts))
    except Exception as exc:
        warehouse.finish_run(run_id, "failed", details=str(exc))
        raise
    return {"run_id": run_id, **counts}
