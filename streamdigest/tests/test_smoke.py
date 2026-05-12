"""Smoke tests — no network, no Ollama required.

Covers:
  - pure coercion of LLM output (`_coerce`)
  - DuckDB schema init + upsert round-trip against a tmp DB
  - prompt assembly (deterministic string build)
"""

from __future__ import annotations

from pathlib import Path

from streamdigest.ai.enrich import _coerce, _input_hash
from streamdigest.ai.prompts import build_user_prompt
from streamdigest.storage.duckdb_store import Store


def test_coerce_clamps_priority_and_normalizes_sentiment():
    obj = {
        "summary": "  hi  ",
        "actions": "single string",
        "priority": 99,
        "priority_reason": "r",
        "sentiment": "GRUMPY",
    }
    summary, actions, priority, reason, sentiment = _coerce(obj)
    assert summary == "hi"
    assert actions == ["single string"]
    assert priority == 5
    assert reason == "r"
    assert sentiment == "neutral"


def test_coerce_handles_missing_fields():
    summary, actions, priority, reason, sentiment = _coerce({})
    assert summary == ""
    assert actions == []
    assert 1 <= priority <= 5
    assert reason == ""
    assert sentiment == "neutral"


def test_input_hash_is_stable_and_sensitive():
    a = _input_hash("title", "body", "reason")
    b = _input_hash("title", "body", "reason")
    c = _input_hash("title", "body", "other")
    assert a == b
    assert a != c


def test_build_user_prompt_truncates_long_bodies():
    out = build_user_prompt(
        repo="o/r", subject_type="Issue", title="t", reason="mention",
        body="x" * 5000, body_char_limit=200,
    )
    assert "…[truncated]" in out
    assert "o/r" in out


def test_storage_roundtrip(tmp_path: Path):
    db = tmp_path / "t.duckdb"
    store = Store(db)
    store.init_schema()
    store.upsert_enrichment(
        id="abc",
        summary="s",
        actions=["a1"],
        priority=3,
        priority_reason="r",
        sentiment="neutral",
        model="test",
        input_hash="h",
    )
    with store.connect() as conn:
        row = conn.execute(
            "SELECT id, summary, priority FROM enriched.notifications"
        ).fetchone()
    assert row == ("abc", "s", 3)
