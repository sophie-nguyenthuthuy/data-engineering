from __future__ import annotations
import asyncio
import json

import redis.asyncio as aioredis
import structlog
from fastapi import WebSocket, WebSocketDisconnect

from ..config import settings

log = structlog.get_logger(__name__)


class ConnectionManager:
    """Fan-out manager for active WebSocket connections."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.add(ws)
        log.info("ws_client_connected", total=len(self._connections))

    def disconnect(self, ws: WebSocket) -> None:
        self._connections.discard(ws)
        log.info("ws_client_disconnected", total=len(self._connections))

    async def broadcast(self, payload: str) -> None:
        dead: list[WebSocket] = []
        for ws in list(self._connections):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._connections.discard(ws)


manager = ConnectionManager()


async def redis_subscription_loop(redis_client: aioredis.Redis) -> None:
    """
    Subscribe to the Redis metrics channel and broadcast every message
    to all connected WebSocket clients. Runs as a background task.
    """
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(settings.redis_metrics_channel)
    log.info("redis_pubsub_subscribed", channel=settings.redis_metrics_channel)

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        data = message["data"]
        if isinstance(data, bytes):
            data = data.decode()
        await manager.broadcast(data)


async def websocket_endpoint(
    ws: WebSocket, redis_client: aioredis.Redis
) -> None:
    """
    Handle a single WebSocket connection.

    On connect, immediately push the latest snapshot so the client is
    not stuck waiting for the next publish cycle.
    """
    await manager.connect(ws)
    try:
        # Send the latest cached snapshot immediately
        latest = await redis_client.get("dq:snapshot:latest")
        if latest:
            await ws.send_text(
                latest.decode() if isinstance(latest, bytes) else latest
            )

        # Keep connection alive; actual data arrives via broadcast
        while True:
            # Ping/pong to detect stale connections
            await asyncio.sleep(30)
            await ws.send_text(json.dumps({"type": "ping"}))

    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(ws)
