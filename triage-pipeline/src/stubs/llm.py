"""Classifier. Uses a local Ollama server if TRIAGE_USE_REAL_LLM=1, else a
deterministic keyword-based mock. Same return contract so the worker doesn't care.

Open-source by default — no API keys. Point OLLAMA_HOST at any Ollama server
(default http://127.0.0.1:11434) and pull the model in OLLAMA_MODEL
(default llama3.2:3b — ~2 GB, fits on a laptop).
"""
from __future__ import annotations

import json
import os
import random
import re
import time
from dataclasses import dataclass

import httpx

DEFAULT_MODEL = "llama3.2:3b"
DEFAULT_HOST = "http://127.0.0.1:11434"


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


def _ollama_classify(subject: str, body: str, labels: list[str]) -> Classification:
    host = os.environ.get("OLLAMA_HOST", DEFAULT_HOST).rstrip("/")
    model = os.environ.get("OLLAMA_MODEL", DEFAULT_MODEL)
    user = (
        f"Allowed labels: {labels}\n"
        f"Subject: {subject}\n"
        f"Body: {body}\n"
    )
    t0 = time.perf_counter()
    resp = httpx.post(
        f"{host}/api/chat",
        timeout=120.0,
        json={
            "model": model,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.0, "num_predict": 256},
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user},
            ],
        },
    )
    resp.raise_for_status()
    latency_ms = int((time.perf_counter() - t0) * 1000)
    text = resp.json()["message"]["content"].strip()
    text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.MULTILINE).strip()
    data = json.loads(text)
    label = data["label"] if data.get("label") in labels else "spam"
    return Classification(
        predicted_label=label,
        confidence=float(data.get("confidence", 0.5)),
        summary=str(data.get("summary", ""))[:200],
        priority=data.get("priority", "low") if data.get("priority") in {"low", "med", "high"} else "low",
        latency_ms=latency_ms,
        model=model,
    )


def classify(subject: str, body: str, labels: list[str]) -> Classification:
    if not body or "INVALID" in body[:32]:
        raise ValueError("empty or poisoned payload")
    if os.environ.get("TRIAGE_USE_REAL_LLM") == "1":
        return _ollama_classify(subject, body, labels)
    return _mock_classify(subject, body, labels)
