"""Shared fixtures."""

from __future__ import annotations

import random

import pytest

from art_mvcc.art.tree import ART
from art_mvcc.mvcc.store import MVCCArt


@pytest.fixture
def art() -> ART:
    return ART()


@pytest.fixture
def db() -> MVCCArt:
    return MVCCArt()


@pytest.fixture
def random_keys() -> list[bytes]:
    rng = random.Random(42)
    return [bytes(rng.randint(0, 255) for _ in range(rng.randint(1, 8)))
            for _ in range(1000)]
