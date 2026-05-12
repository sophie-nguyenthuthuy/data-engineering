"""Synthetic event generation. Kept deterministic-enough to be testable."""

from __future__ import annotations

import random
import time
import uuid
from typing import Any

_COUNTRIES = ["US", "GB", "DE", "FR", "JP", "BR", "IN", "CA", "AU", "VN"]
_DEVICES = ["desktop", "mobile", "tablet"]
_EVENT_TYPES = ["page_view", "click", "add_to_cart", "checkout", "purchase", "search"]
_ERROR_CODES = ["E_TIMEOUT", "E_VALIDATION", "E_AUTH", "E_5XX", "E_RATE_LIMIT"]

_DEFAULT_RNG = random.Random()


def make_event(*, error_rate: float, rng: random.Random | None = None) -> dict[str, Any]:
    r: random.Random = rng if rng is not None else _DEFAULT_RNG
    is_error = r.random() < error_rate
    event_type = r.choice(_EVENT_TYPES)
    return {
        "event_id": str(uuid.uuid4()),
        "occurred_at": int(time.time() * 1000),
        "user_id": f"u_{r.randint(1, 10_000)}",
        "session_id": f"s_{r.randint(1, 100_000)}",
        "event_type": event_type,
        "status": "error" if is_error else "success",
        "error_code": r.choice(_ERROR_CODES) if is_error else None,
        "latency_ms": _latency_for(event_type, is_error, r),
        "country": r.choice(_COUNTRIES),
        "device": r.choice(_DEVICES),
        "metadata": {"src": "synth"},
    }


_LATENCY_BASE = {
    "page_view": 80,
    "click": 30,
    "add_to_cart": 120,
    "checkout": 300,
    "purchase": 500,
    "search": 150,
}


def _latency_for(event_type: str, is_error: bool, r: random.Random) -> int:
    base = _LATENCY_BASE[event_type]
    jitter = r.gauss(0, base * 0.25)
    tail = r.expovariate(1 / (base * 2)) if r.random() < 0.02 else 0
    err_penalty = r.uniform(500, 3000) if is_error else 0
    return max(1, int(base + jitter + tail + err_penalty))
