"""Classifier. Uses Claude if ANTHROPIC_API_KEY is set, else deterministic
keyword-based mock. Same return contract so the worker doesn't care.
"""
from __future__ import annotations

import json
import os
import random
import re
import time
from dataclasses import dataclass

MODEL_ID = "claude-haiku-4-5-20251001"


@dataclass
class Classification:
    predicted_label: str
    confidence: float
    summary: str
    priority: str  # low | med | high
    latency_ms: int
    model: str


SYSTEM_PROMPT = """You triage customer emails for a multi-tenant SaaS. Reply with ONLY a
JSON object, no prose, no markdown:
{"label": "...", "confidence": 0.0-1.0, "summary": "<=20 words", "priority": "low|med|high"}
Pick the label from the provided list. Priority=high for outages, security,
legal deadlines, or customer churn risk. Never invent labels outside the list."""


def _mock_classify(subject: str, body: str, labels: list[str]) -> Classification:
    text = f"{subject} {body}".lower()
    rules = {
        "urgent": ["down", "outage", "502", "500", "security", "incident", "now"],
        "billing": ["invoice", "refund", "charge", "card", "subscription", "payment"],
        "support": ["error", "bug", "help", "broken", "500", "dashboard", "export"],
        "sales": ["demo", "pricing", "contract", "proposal", "seats", "enterprise"],
        "legal": ["dpa", "subpoena", "counsel", "legal"],
        "hr": ["resignation", "benefits", "enrollment", "employee"],
        "spam": ["congratulations", "winner", "prize", "cheap meds", "click here"],
    }
    scores = {lbl: sum(1 for kw in kws if kw in text) for lbl, kws in rules.items() if lbl in labels}
    if not scores or max(scores.values()) == 0:
        label = random.choice(labels)
        conf = 0.35
    else:
        label = max(scores, key=scores.get)
        conf = min(0.95, 0.55 + 0.1 * scores[label])
    priority = "high" if label in {"urgent", "legal"} else ("med" if label in {"billing", "support"} else "low")
    summary = re.sub(r"\s+", " ", body)[:120]
    return Classification(label, conf, summary, priority, latency_ms=random.randint(40, 120), model="mock")


def _claude_classify(subject: str, body: str, labels: list[str]) -> Classification:
    # Keep the import local so the mock path stays dependency-free at import time.
    from anthropic import Anthropic

    client = Anthropic()  # reads ANTHROPIC_API_KEY
    user = (
        f"Allowed labels: {labels}\n"
        f"Subject: {subject}\n"
        f"Body: {body}\n"
    )
    t0 = time.perf_counter()
    resp = client.messages.create(
        model=MODEL_ID,
        max_tokens=256,
        temperature=0.0,
        system=[
            {"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}},
        ],
        messages=[{"role": "user", "content": user}],
    )
    latency_ms = int((time.perf_counter() - t0) * 1000)
    text = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text").strip()
    # Strip markdown fences defensively.
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.MULTILINE).strip()
    data = json.loads(text)
    label = data["label"] if data["label"] in labels else "spam"
    return Classification(
        predicted_label=label,
        confidence=float(data.get("confidence", 0.5)),
        summary=str(data.get("summary", ""))[:200],
        priority=data.get("priority", "low"),
        latency_ms=latency_ms,
        model=MODEL_ID,
    )


def classify(subject: str, body: str, labels: list[str]) -> Classification:
    if not body or "INVALID" in body[:32]:
        raise ValueError("empty or poisoned payload")
    if os.environ.get("ANTHROPIC_API_KEY") and os.environ.get("TRIAGE_USE_REAL_LLM") == "1":
        return _claude_classify(subject, body, labels)
    return _mock_classify(subject, body, labels)
