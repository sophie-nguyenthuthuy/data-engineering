"""Enrichment orchestration: one notification in, structured fields out."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from streamdigest.ai.ollama_client import OllamaClient, OllamaError
from streamdigest.ai.prompts import ENRICH_SYSTEM, build_user_prompt


@dataclass
class Enrichment:
    summary: str
    actions: list[str]
    priority: int
    priority_reason: str
    sentiment: str
    model: str
    input_hash: str


_VALID_SENTIMENTS = {"positive", "neutral", "negative", "mixed"}


def _input_hash(*parts: str | None) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()[:16]


def _coerce(obj: dict[str, Any]) -> tuple[str, list[str], int, str, str]:
    """Defensive coercion — local models sometimes wander from the schema."""
    summary = str(obj.get("summary", "")).strip()[:400]

    raw_actions = obj.get("actions") or []
    if isinstance(raw_actions, str):
        raw_actions = [raw_actions]
    actions = [str(a).strip() for a in raw_actions if str(a).strip()][:3]

    try:
        priority = int(obj.get("priority", 2))
    except (TypeError, ValueError):
        priority = 2
    priority = max(1, min(5, priority))

    priority_reason = str(obj.get("priority_reason", "")).strip()[:200]

    sentiment = str(obj.get("sentiment", "neutral")).strip().lower()
    if sentiment not in _VALID_SENTIMENTS:
        sentiment = "neutral"

    return summary, actions, priority, priority_reason, sentiment


def enrich_one(row: dict[str, Any], client: OllamaClient) -> Enrichment:
    """Enrich one notification row. Raises OllamaError on failure."""
    user = build_user_prompt(
        repo=row.get("repo_full_name"),
        subject_type=row.get("subject_type"),
        title=row.get("subject_title"),
        reason=row.get("reason"),
        body=row.get("body"),
    )
    ihash = _input_hash(row.get("subject_title"), row.get("body"), row.get("reason"))

    obj = client.chat_json(system=ENRICH_SYSTEM, user=user)
    summary, actions, priority, priority_reason, sentiment = _coerce(obj)

    if not summary:
        raise OllamaError("Enrichment produced an empty summary")

    return Enrichment(
        summary=summary,
        actions=actions,
        priority=priority,
        priority_reason=priority_reason,
        sentiment=sentiment,
        model=client.model,
        input_hash=ihash,
    )
