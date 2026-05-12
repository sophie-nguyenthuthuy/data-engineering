"""Backpressure signaling bus.

Two implementations:
  - InMemoryBus  : zero-dependency, suitable for single-process demos / tests
  - RedisBus     : pub/sub over Redis, suitable for distributed deployments
"""
from __future__ import annotations

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Coroutine

from .metrics import BackpressureSignal, ThrottleCommand

logger = logging.getLogger(__name__)

_SIGNAL_CHANNEL = "bp:signal"
_THROTTLE_CHANNEL_PREFIX = "bp:throttle:"


SignalCallback = Callable[[BackpressureSignal], Coroutine[Any, Any, None]]
ThrottleCallback = Callable[[ThrottleCommand], Coroutine[Any, Any, None]]


class BackpressureBus(ABC):
    @abstractmethod
    async def publish_signal(self, signal: BackpressureSignal) -> None: ...

    @abstractmethod
    async def publish_throttle(self, cmd: ThrottleCommand) -> None: ...

    @abstractmethod
    async def subscribe_signals(self, callback: SignalCallback) -> None: ...

    @abstractmethod
    async def subscribe_throttle(self, job_id: str, callback: ThrottleCallback) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...


class InMemoryBus(BackpressureBus):
    """Single-process event bus backed by asyncio queues."""

    def __init__(self) -> None:
        self._signal_subs: list[SignalCallback] = []
        self._throttle_subs: dict[str, list[ThrottleCallback]] = {}
        self._lock = asyncio.Lock()

    async def publish_signal(self, signal: BackpressureSignal) -> None:
        logger.debug("BUS signal from %s level=%s", signal.source_job_id, signal.level.name)
        async with self._lock:
            cbs = list(self._signal_subs)
        await asyncio.gather(*(cb(signal) for cb in cbs), return_exceptions=True)

    async def publish_throttle(self, cmd: ThrottleCommand) -> None:
        logger.debug("BUS throttle → %s factor=%.2f", cmd.target_job_id, cmd.throttle_factor)
        async with self._lock:
            cbs = list(self._throttle_subs.get(cmd.target_job_id, []))
        await asyncio.gather(*(cb(cmd) for cb in cbs), return_exceptions=True)

    async def subscribe_signals(self, callback: SignalCallback) -> None:
        async with self._lock:
            self._signal_subs.append(callback)

    async def subscribe_throttle(self, job_id: str, callback: ThrottleCallback) -> None:
        async with self._lock:
            self._throttle_subs.setdefault(job_id, []).append(callback)

    async def close(self) -> None:
        async with self._lock:
            self._signal_subs.clear()
            self._throttle_subs.clear()


class RedisBus(BackpressureBus):
    """Distributed bus using Redis pub/sub. Requires `redis[hiredis]`."""

    def __init__(self, redis_url: str = "redis://localhost:6379") -> None:
        try:
            import redis.asyncio as aioredis
        except ImportError as exc:
            raise ImportError("Install redis[hiredis] to use RedisBus") from exc

        self._redis_url = redis_url
        self._aioredis = aioredis
        self._publisher = aioredis.from_url(redis_url, decode_responses=True)
        self._subscriber_conn = aioredis.from_url(redis_url, decode_responses=True)
        self._pubsub = self._subscriber_conn.pubsub()
        self._listener_task: asyncio.Task | None = None

    async def publish_signal(self, signal: BackpressureSignal) -> None:
        await self._publisher.publish(_SIGNAL_CHANNEL, json.dumps(signal.to_dict()))

    async def publish_throttle(self, cmd: ThrottleCommand) -> None:
        channel = _THROTTLE_CHANNEL_PREFIX + cmd.target_job_id
        await self._publisher.publish(channel, json.dumps(cmd.to_dict()))

    async def subscribe_signals(self, callback: SignalCallback) -> None:
        await self._pubsub.subscribe(**{_SIGNAL_CHANNEL: self._make_signal_handler(callback)})
        self._ensure_listener()

    async def subscribe_throttle(self, job_id: str, callback: ThrottleCallback) -> None:
        channel = _THROTTLE_CHANNEL_PREFIX + job_id
        await self._pubsub.subscribe(**{channel: self._make_throttle_handler(callback)})
        self._ensure_listener()

    def _make_signal_handler(self, cb: SignalCallback):
        async def handler(msg):
            if msg["type"] == "message":
                signal = BackpressureSignal.from_dict(json.loads(msg["data"]))
                await cb(signal)
        return handler

    def _make_throttle_handler(self, cb: ThrottleCallback):
        async def handler(msg):
            if msg["type"] == "message":
                cmd = ThrottleCommand.from_dict(json.loads(msg["data"]))
                await cb(cmd)
        return handler

    def _ensure_listener(self) -> None:
        if self._listener_task is None or self._listener_task.done():
            self._listener_task = asyncio.create_task(self._listen())

    async def _listen(self) -> None:
        async for _ in self._pubsub.listen():
            pass

    async def close(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
        await self._pubsub.close()
        await self._publisher.aclose()
        await self._subscriber_conn.aclose()
