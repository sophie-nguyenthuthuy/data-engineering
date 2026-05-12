"""Compute-side page cache.

Cache is per-client. On miss, fetch from page server via Transport. On a
write that the page server reports causes invalidations elsewhere, we
notify each other client (in a real system the server would push the
invalidations; we simulate it via the registry).
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from disagg.core.page import Page, PageId

if TYPE_CHECKING:
    from disagg.transport.api import Transport


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    writes: int = 0
    invalidations_received: int = 0


class ClientCache:
    """Per-client local cache with LRU eviction."""

    def __init__(
        self,
        client_id: int,
        transport: Transport,
        capacity: int = 64,
        invalidation_registry: InvalidationRegistry | None = None,
    ) -> None:
        self.client_id = client_id
        self.transport = transport
        self.capacity = capacity
        self.invalidation_registry = invalidation_registry
        if invalidation_registry is not None:
            invalidation_registry.register(self.client_id, self)
        self._cache: OrderedDict[PageId, Page] = OrderedDict()
        self._lock = threading.RLock()
        self.stats = CacheStats()

    # ---- Public API -------------------------------------------------------

    def read(self, page_id: PageId) -> Page:
        with self._lock:
            cached = self._cache.get(page_id)
            if cached is not None:
                self.stats.hits += 1
                self._cache.move_to_end(page_id)
                return cached
            self.stats.misses += 1
        # Miss — fetch outside lock to avoid blocking other readers
        page = self.transport.call("read", client_id=self.client_id, page_id=page_id)
        with self._lock:
            self._cache[page_id] = page
            self._cache.move_to_end(page_id)
            self._evict_locked()
        return page

    def write(self, page_id: PageId, data: bytes) -> None:
        self.stats.writes += 1
        resp = self.transport.call(
            "write", client_id=self.client_id, page_id=page_id, data=data,
        )
        invalidate_ids: list[int] = resp.get("invalidate", []) if isinstance(resp, dict) else []
        # Update our own cache to the new page (we just wrote it)
        with self._lock:
            new_page = Page(page_id=page_id, version=0, data=data)
            existing = self._cache.get(page_id)
            new_page.version = (existing.version + 1) if existing else 1
            self._cache[page_id] = new_page
            self._cache.move_to_end(page_id)
            self._evict_locked()
        # Push invalidations to other clients
        if self.invalidation_registry is not None:
            for cid in invalidate_ids:
                self.invalidation_registry.invalidate(cid, page_id)

    def invalidate(self, page_id: PageId) -> None:
        with self._lock:
            self._cache.pop(page_id, None)
            self.stats.invalidations_received += 1

    def release(self, page_id: PageId) -> None:
        self.transport.call("release", client_id=self.client_id, page_id=page_id)
        with self._lock:
            self._cache.pop(page_id, None)

    # ---- Internal ---------------------------------------------------------

    def _evict_locked(self) -> None:
        while len(self._cache) > self.capacity:
            self._cache.popitem(last=False)

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._cache)


# ---------------------------------------------------------------------------
# Invalidation registry: in a real system the server pushes invalidations
# via a control plane. We model it as an in-process registry that the
# writer-client calls into after the server tells it which clients to
# notify.
# ---------------------------------------------------------------------------


class InvalidationRegistry:
    def __init__(self) -> None:
        self._clients: dict[int, ClientCache] = {}
        self._lock = threading.Lock()

    def register(self, client_id: int, cache: ClientCache) -> None:
        with self._lock:
            self._clients[client_id] = cache

    def unregister(self, client_id: int) -> None:
        with self._lock:
            self._clients.pop(client_id, None)

    def invalidate(self, client_id: int, page_id: PageId) -> None:
        with self._lock:
            client = self._clients.get(client_id)
        if client is not None:
            client.invalidate(page_id)
