"""Shared pytest fixtures for all test modules."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import pytest

from src.lambda_arch.models import Event


def _make_event(
    event_type: str = "purchase",
    amount: float = 100.0,
    user_id: str = "user_0001",
    timestamp: datetime | None = None,
) -> Event:
    return Event(
        event_id=str(uuid.uuid4()),
        user_id=user_id,
        event_type=event_type,
        amount=amount,
        timestamp=timestamp or datetime(2024, 1, 3, 10, 0, 0),
    )


@pytest.fixture
def sample_events() -> list[Event]:
    """100 synthetic events for unit tests — no I/O required."""
    base = datetime(2024, 1, 1, 8, 0, 0)
    events: list[Event] = []
    types = ["purchase", "view", "click", "signup"]
    weights = [10, 60, 25, 5]

    import random
    rng = random.Random(99)

    for i in range(100):
        etype = rng.choices(types, weights=weights, k=1)[0]
        amount = round(rng.uniform(1, 300), 2) if etype == "purchase" else 0.0
        ts = base + timedelta(hours=rng.randint(0, 6 * 24), minutes=rng.randint(0, 59))
        events.append(
            Event(
                event_id=str(uuid.uuid4()),
                user_id=f"user_{(i % 20):04d}",
                event_type=etype,
                amount=amount,
                timestamp=ts,
            )
        )
    return events


@pytest.fixture
def make_event():
    """Factory fixture to create individual test events."""
    return _make_event
