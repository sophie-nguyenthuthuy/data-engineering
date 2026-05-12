"""
Distributed key-value store state machine on top of Raft.

Operations:
  PUT key value [version]  — optimistic write with version check
  GET key                  — point read (always through leader for linearizability)
  DELETE key
  CAS key expected_val new_val   — compare-and-swap
  LIST prefix              — scan by prefix
  WATCH key                — not replicated; handled at server level
"""

import asyncio
import fnmatch
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class VersionedValue:
    __slots__ = ("value", "version", "created_at", "updated_at")

    def __init__(self, value: Any, version: int = 1):
        self.value = value
        self.version = version
        self.created_at = time.time()
        self.updated_at = time.time()

    def to_dict(self) -> dict:
        return {
            "value": self.value,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "VersionedValue":
        v = cls(d["value"], d["version"])
        v.created_at = d["created_at"]
        v.updated_at = d["updated_at"]
        return v


class KVStore:
    """
    State machine: a versioned in-memory key-value store.
    All mutations go through Raft; reads served from local state on leader.
    """

    def __init__(self) -> None:
        self._data: Dict[str, VersionedValue] = {}
        self._watchers: Dict[str, Set[asyncio.Queue]] = {}
        self._lock = asyncio.Lock()

    # ── State machine interface ───────────────────────────────────────────

    async def apply(self, command: Dict[str, Any]) -> Any:
        """Called by RaftNode for each committed log entry."""
        op = command.get("op")
        if op == "put":
            return await self._put(command["key"], command["value"], command.get("version"))
        elif op == "delete":
            return await self._delete(command["key"])
        elif op == "cas":
            return await self._cas(
                command["key"], command["expected"], command["new_value"]
            )
        elif op in ("_noop", "_config_joint", "_config_final", "_config_remove"):
            return None
        else:
            raise ValueError(f"unknown op: {op}")

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return {k: v.to_dict() for k, v in self._data.items()}

    async def restore(self, data: Dict[str, Any]) -> None:
        async with self._lock:
            self._data = {k: VersionedValue.from_dict(v) for k, v in data.items()}

    # ── Read ops (local, no Raft round-trip) ─────────────────────────────

    async def get(self, key: str) -> Optional[VersionedValue]:
        async with self._lock:
            return self._data.get(key)

    async def list_prefix(self, prefix: str) -> List[Tuple[str, VersionedValue]]:
        async with self._lock:
            return [
                (k, v) for k, v in sorted(self._data.items()) if k.startswith(prefix)
            ]

    async def list_glob(self, pattern: str) -> List[Tuple[str, VersionedValue]]:
        async with self._lock:
            return [
                (k, v)
                for k, v in sorted(self._data.items())
                if fnmatch.fnmatch(k, pattern)
            ]

    # ── Watch support (local subscription, no consensus needed) ──────────

    def subscribe(self, key: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._watchers.setdefault(key, set()).add(q)
        return q

    def unsubscribe(self, key: str, q: asyncio.Queue) -> None:
        watchers = self._watchers.get(key, set())
        watchers.discard(q)

    # ── Private write ops ─────────────────────────────────────────────────

    async def _put(
        self, key: str, value: Any, expected_version: Optional[int]
    ) -> Dict[str, Any]:
        async with self._lock:
            existing = self._data.get(key)
            if expected_version is not None:
                cur_ver = existing.version if existing else 0
                if cur_ver != expected_version:
                    return {
                        "ok": False,
                        "error": f"version conflict: expected {expected_version}, got {cur_ver}",
                    }
            if existing:
                existing.value = value
                existing.version += 1
                existing.updated_at = time.time()
                vv = existing
            else:
                vv = VersionedValue(value)
                self._data[key] = vv

            self._notify(key, {"op": "put", "key": key, "vv": vv.to_dict()})
            return {"ok": True, "version": vv.version}

    async def _delete(self, key: str) -> Dict[str, Any]:
        async with self._lock:
            if key not in self._data:
                return {"ok": False, "error": "key not found"}
            del self._data[key]
            self._notify(key, {"op": "delete", "key": key})
            return {"ok": True}

    async def _cas(
        self, key: str, expected: Any, new_value: Any
    ) -> Dict[str, Any]:
        async with self._lock:
            existing = self._data.get(key)
            current = existing.value if existing else None
            if current != expected:
                return {
                    "ok": False,
                    "error": f"CAS failed: expected {expected!r}, got {current!r}",
                }
            if existing:
                existing.value = new_value
                existing.version += 1
                existing.updated_at = time.time()
                vv = existing
            else:
                vv = VersionedValue(new_value)
                self._data[key] = vv
            self._notify(key, {"op": "put", "key": key, "vv": vv.to_dict()})
            return {"ok": True, "version": vv.version}

    def _notify(self, key: str, event: dict) -> None:
        for q in list(self._watchers.get(key, set())):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass
