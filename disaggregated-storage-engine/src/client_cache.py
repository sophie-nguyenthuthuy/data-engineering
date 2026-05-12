"""Compute-side page cache. On read miss, fetch from page server."""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock

from .page_server import PageServer, PAGE_SIZE


@dataclass
class ClientCache:
    client_id: int
    server: PageServer
    capacity: int = 64
    _cache: OrderedDict = None
    _lock: Lock = None

    def __post_init__(self):
        self._cache = OrderedDict()
        self._lock = Lock()
        self.stats = {"local_hits": 0, "local_misses": 0, "invalidations_recv": 0}

    def read(self, page_id: int) -> bytes:
        with self._lock:
            if page_id in self._cache:
                self.stats["local_hits"] += 1
                self._cache.move_to_end(page_id)
                return self._cache[page_id]
            self.stats["local_misses"] += 1
        # Miss: fetch from server (outside our lock)
        page = self.server.read(self.client_id, page_id)
        with self._lock:
            self._cache[page_id] = page
            self._cache.move_to_end(page_id)
            self._evict_if_full()
        return page

    def write(self, page_id: int, data: bytes) -> None:
        assert len(data) == PAGE_SIZE
        invalidate = self.server.write(self.client_id, page_id, data)
        # Update local cache
        with self._lock:
            self._cache[page_id] = data
            self._cache.move_to_end(page_id)
            self._evict_if_full()
        # In a real system, server would push invalidations; we just record.
        return invalidate

    def invalidate(self, page_id: int) -> None:
        with self._lock:
            self._cache.pop(page_id, None)
            self.stats["invalidations_recv"] += 1

    def _evict_if_full(self) -> None:
        while len(self._cache) > self.capacity:
            self._cache.popitem(last=False)

    @property
    def size(self) -> int:
        return len(self._cache)


__all__ = ["ClientCache"]
