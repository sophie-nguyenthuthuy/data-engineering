"""Disaggregated storage engine.

A compute-storage-separated database storage engine: the buffer pool lives
on a remote `PageServer`, compute nodes share it via a coherence protocol,
and a Markov-chain prefetcher hides network latency on sequential access.

Public API:
    from disagg import PageServer, ClientCache, MarkovPrefetcher
"""

from __future__ import annotations

__version__ = "0.1.0"

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from disagg.client.cache import ClientCache
    from disagg.core.page import Page, PageId
    from disagg.prefetch.markov import MarkovPrefetcher
    from disagg.server.page_server import PageServer

_LAZY: dict[str, str] = {
    "PageServer": "disagg.server.page_server",
    "ClientCache": "disagg.client.cache",
    "MarkovPrefetcher": "disagg.prefetch.markov",
    "Page": "disagg.core.page",
    "PageId": "disagg.core.page",
}


def __getattr__(name: str) -> Any:
    mod = _LAZY.get(name)
    if mod is None:
        raise AttributeError(f"module 'disagg' has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(mod), name)


__all__ = ["ClientCache", "MarkovPrefetcher", "Page", "PageId", "PageServer"]
