#!/usr/bin/env python3
"""Generate synthetic historical events and write them to data/historical/."""

from __future__ import annotations

import json
import logging
import random
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# Make sure src is importable when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import HISTORICAL_DIR

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------
NUM_EVENTS = 10_000
NUM_USERS = 100
EVENT_TYPES = ["purchase", "view", "click", "signup"]
# Realistic distribution: views >> clicks >> purchases >> signups
EVENT_WEIGHTS = [5, 55, 35, 5]
START_DATE = datetime(2024, 1, 1)
NUM_DAYS = 7

AMOUNT_RANGES = {
    "purchase": (1.0, 500.0),
    "view": (0.0, 0.0),
    "click": (0.0, 0.0),
    "signup": (0.0, 0.0),
}


def generate_events(num_events: int = NUM_EVENTS) -> list[dict]:
    """Generate a list of synthetic event dicts."""
    users = [f"user_{i:04d}" for i in range(NUM_USERS)]
    events: list[dict] = []

    for _ in range(num_events):
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        lo, hi = AMOUNT_RANGES[event_type]
        amount = round(random.uniform(lo, hi), 2) if hi > 0 else 0.0

        # Spread timestamps across 7 days with realistic intra-day patterns
        day_offset = random.randint(0, NUM_DAYS - 1)
        # Weight towards business hours (8-22)
        hour = random.choices(
            range(24),
            weights=[1, 1, 1, 1, 1, 1, 1, 1, 3, 4, 5, 6, 6, 6, 5, 5, 5, 5, 4, 4, 3, 2, 1, 1],
            k=1,
        )[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        ts = START_DATE + timedelta(days=day_offset, hours=hour, minutes=minute, seconds=second)

        events.append(
            {
                "event_id": str(uuid.uuid4()),
                "user_id": random.choice(users),
                "event_type": event_type,
                "amount": amount,
                "timestamp": ts.isoformat(),
                "metadata": {},
            }
        )

    return events


def save_events(events: list[dict], output_dir: Path) -> dict[str, int]:
    """Split events by date and save to daily JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Group by date
    by_date: dict[str, list[dict]] = {}
    for e in events:
        date_key = e["timestamp"][:10]  # "2024-01-01"
        by_date.setdefault(date_key, []).append(e)

    counts: dict[str, int] = {}
    for date_key, day_events in sorted(by_date.items()):
        path = output_dir / f"{date_key}.json"
        with open(path, "w") as fh:
            json.dump(day_events, fh, indent=2)
        counts[date_key] = len(day_events)
        logger.info("Wrote %d events to %s", len(day_events), path)

    return counts


def main() -> None:
    random.seed(42)
    logger.info("Generating %d synthetic events across %d days...", NUM_EVENTS, NUM_DAYS)
    events = generate_events(NUM_EVENTS)
    counts = save_events(events, HISTORICAL_DIR)
    total = sum(counts.values())
    logger.info("Done — %d events in %d files", total, len(counts))


if __name__ == "__main__":
    main()
