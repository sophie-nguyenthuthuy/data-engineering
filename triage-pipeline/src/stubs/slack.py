"""Slack sink stub — appends to a JSONL file per tenant so the dashboard can
replay what would have been posted. Swap for httpx.post(webhook_url, ...) in prod.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from ..config import DATA_DIR


def post(tenant_id: str, channel: str, text: str, blocks: list | None = None) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "tenant_id": tenant_id,
        "channel": channel,
        "text": text,
        "blocks": blocks or [],
    }
    with (DATA_DIR / "slack_outbox.jsonl").open("a") as fh:
        fh.write(json.dumps(record) + "\n")


def recent(limit: int = 25) -> list[dict]:
    path = DATA_DIR / "slack_outbox.jsonl"
    if not path.exists():
        return []
    with path.open() as fh:
        lines = fh.readlines()
    return [json.loads(l) for l in lines[-limit:]][::-1]
