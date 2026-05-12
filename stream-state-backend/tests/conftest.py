"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from ssb.backend.memory_backend import MemoryBackend
from ssb.manager import StateBackendManager


@pytest.fixture
def backend() -> MemoryBackend:
    """An opened MemoryBackend ready for use."""
    b = MemoryBackend()
    b.open()
    yield b
    b.close()


@pytest.fixture
def manager() -> StateBackendManager:
    """A started StateBackendManager using the in-memory backend."""
    mgr = StateBackendManager(backend="memory")
    mgr.start()
    yield mgr
    mgr.stop()
