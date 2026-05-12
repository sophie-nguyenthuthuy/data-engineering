"""
Synthetic data generator.

Generates realistic transaction events.  After DRIFT_AFTER_EVENTS events,
it shifts the distribution to simulate training-serving skew:
  - amounts shift from ~$250 to ~$1500 (mean drift)
  - category distribution shifts toward high-risk categories
  - user age distribution skews younger
"""
from __future__ import annotations

import os
import random
from datetime import datetime, timezone


CATEGORIES_NORMAL = [
    "groceries", "dining", "travel", "entertainment", "utilities",
    "retail", "healthcare", "education", "subscriptions", "gas",
]
CATEGORIES_DRIFTED = [
    "gambling", "crypto", "forex", "adult", "gaming",
    "luxury", "travel", "entertainment", "retail", "dining",
]

DRIFT_AFTER_EVENTS = int(os.getenv("DRIFT_AFTER_EVENTS", "500"))


def generate_event(seq_number: int) -> dict:
    """Generate one raw transaction event. Distribution shifts after seq_number > DRIFT_AFTER_EVENTS."""
    drifted = seq_number > DRIFT_AFTER_EVENTS

    if drifted:
        amount = max(0.01, random.lognormvariate(7.0, 1.2))  # mean ~$1500
        categories = CATEGORIES_DRIFTED
        user_age = random.randint(18, 35)
    else:
        amount = max(0.01, random.lognormvariate(5.5, 1.0))  # mean ~$250
        categories = CATEGORIES_NORMAL
        user_age = random.randint(20, 70)

    user_id = f"user_{random.randint(1, 1000):04d}"
    days_old = random.randint(30, 2000)
    account_created_ts = datetime.now(tz=timezone.utc).timestamp() - days_old * 86400

    return {
        "user_id": user_id,
        "amount": round(amount, 2),
        "category": random.choice(categories),
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "user_age": user_age,
        "account_created_at": datetime.fromtimestamp(
            account_created_ts, tz=timezone.utc
        ).isoformat(),
        "seq": seq_number,
        "drifted": drifted,
    }


def generate_batch(n: int, start_seq: int = 0) -> list[dict]:
    return [generate_event(start_seq + i) for i in range(n)]
