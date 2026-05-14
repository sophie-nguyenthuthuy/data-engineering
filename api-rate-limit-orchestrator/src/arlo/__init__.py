"""api-rate-limit-orchestrator — distributed token-bucket rate limiter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from arlo.bucket import AcquireResult, TokenBucket
    from arlo.orchestrator import Orchestrator
    from arlo.quota import Quota
    from arlo.storage.base import StorageBackend
    from arlo.storage.inmemory import InMemoryStorage
    from arlo.storage.redis_lua import REDIS_TOKEN_BUCKET_LUA, render_redis_lua


_LAZY: dict[str, tuple[str, str]] = {
    "Quota": ("arlo.quota", "Quota"),
    "StorageBackend": ("arlo.storage.base", "StorageBackend"),
    "InMemoryStorage": ("arlo.storage.inmemory", "InMemoryStorage"),
    "TokenBucket": ("arlo.bucket", "TokenBucket"),
    "AcquireResult": ("arlo.bucket", "AcquireResult"),
    "Orchestrator": ("arlo.orchestrator", "Orchestrator"),
    "REDIS_TOKEN_BUCKET_LUA": ("arlo.storage.redis_lua", "REDIS_TOKEN_BUCKET_LUA"),
    "render_redis_lua": ("arlo.storage.redis_lua", "render_redis_lua"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "REDIS_TOKEN_BUCKET_LUA",
    "AcquireResult",
    "InMemoryStorage",
    "Orchestrator",
    "Quota",
    "StorageBackend",
    "TokenBucket",
    "__version__",
    "render_redis_lua",
]
