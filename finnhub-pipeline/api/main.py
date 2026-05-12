"""
FastAPI backend.

- WebSocket /ws  : streams live trades + 5s aggregates to the dashboard.
                  Backed by aiokafka consumers that fan out to every
                  connected client via an in-memory asyncio broadcast.

- REST /symbols        : distinct symbols seen in TimescaleDB.
- REST /history/{sym}  : recent aggregate history for a symbol (for
                        chart backfill on page load).
- REST /healthz        : liveness.

No polling anywhere on the hot path — trades land in the browser within
one Kafka hop of the producer.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from typing import Any

import asyncpg
import orjson
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

log = logging.getLogger("api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TRADES_TOPIC = os.environ["TRADES_TOPIC"]
AGGREGATES_TOPIC = os.environ["AGGREGATES_TOPIC"]

PG_HOST = os.environ["POSTGRES_HOST"]
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB = os.environ["POSTGRES_DB"]
PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]


class Broadcaster:
    """Tiny fan-out: every connected WS gets its own bounded queue, slow
    consumers drop rather than back-pressure the Kafka reader."""

    def __init__(self) -> None:
        self._subs: set[asyncio.Queue[bytes]] = set()
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue[bytes]:
        q: asyncio.Queue[bytes] = asyncio.Queue(maxsize=500)
        async with self._lock:
            self._subs.add(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue[bytes]) -> None:
        async with self._lock:
            self._subs.discard(q)

    async def publish(self, msg: bytes) -> None:
        # snapshot to avoid holding the lock while putting
        async with self._lock:
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                # drop-oldest for slow consumers
                with contextlib.suppress(asyncio.QueueEmpty):
                    q.get_nowait()
                with contextlib.suppress(asyncio.QueueFull):
                    q.put_nowait(msg)


broadcaster = Broadcaster()
app = FastAPI(default_response_class=ORJSONResponse, title="finnhub-pipeline-api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

pg_pool: asyncpg.Pool | None = None
kafka_task: asyncio.Task | None = None


async def connect_pg() -> asyncpg.Pool:
    delay = 1.0
    while True:
        try:
            pool = await asyncpg.create_pool(
                host=PG_HOST, port=PG_PORT, database=PG_DB,
                user=PG_USER, password=PG_PASSWORD,
                min_size=1, max_size=5,
            )
            log.info("postgres pool ready")
            return pool
        except Exception as e:
            log.warning("postgres not ready (%s), retrying in %.1fs", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10.0)


async def kafka_pump() -> None:
    """Single consumer subscribed to both trade + aggregate topics,
    forwards a tagged JSON envelope to every subscribed WebSocket."""
    delay = 1.0
    while True:
        consumer = AIOKafkaConsumer(
            TRADES_TOPIC,
            AGGREGATES_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=None,
            auto_offset_reset="latest",
            enable_auto_commit=False,
        )
        try:
            await consumer.start()
            log.info("kafka consumer ready (%s, %s)", TRADES_TOPIC, AGGREGATES_TOPIC)
            delay = 1.0
            async for rec in consumer:
                try:
                    payload = orjson.loads(rec.value)
                except Exception:
                    continue
                tag = "trade" if rec.topic == TRADES_TOPIC else "agg"
                envelope = orjson.dumps({"type": tag, "data": payload})
                await broadcaster.publish(envelope)
        except KafkaConnectionError as e:
            log.warning("kafka consumer disconnected (%s), retrying in %.1fs", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15.0)
        except Exception:
            log.exception("kafka pump crashed, restarting")
            await asyncio.sleep(2.0)
        finally:
            with contextlib.suppress(Exception):
                await consumer.stop()


@app.on_event("startup")
async def _startup() -> None:
    global pg_pool, kafka_task
    pg_pool = await connect_pg()
    kafka_task = asyncio.create_task(kafka_pump())


@app.on_event("shutdown")
async def _shutdown() -> None:
    if kafka_task:
        kafka_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await kafka_task
    if pg_pool:
        await pg_pool.close()


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    ok = True
    db_ok = False
    try:
        assert pg_pool is not None
        async with pg_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_ok = True
    except Exception:
        ok = False
    return {"ok": ok, "db": db_ok}


@app.get("/symbols")
async def symbols() -> list[str]:
    assert pg_pool is not None
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT symbol FROM trades WHERE ts > NOW() - INTERVAL '1 hour' ORDER BY symbol"
        )
    return [r["symbol"] for r in rows]


@app.get("/history/{symbol}")
async def history(symbol: str, minutes: int = 15) -> dict[str, Any]:
    minutes = max(1, min(minutes, 240))
    assert pg_pool is not None
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT window_start, window_end, trade_count, avg_price,
                   min_price, max_price, total_volume, vwap
            FROM trades_agg_5s
            WHERE symbol = $1 AND window_start > NOW() - ($2::TEXT || ' minutes')::INTERVAL
            ORDER BY window_start ASC
            """,
            symbol, str(minutes),
        )
    return {
        "symbol": symbol,
        "bars": [
            {
                "window_start": r["window_start"].isoformat(),
                "window_end":   r["window_end"].isoformat(),
                "trade_count":  r["trade_count"],
                "avg_price":    float(r["avg_price"]),
                "min_price":    float(r["min_price"]),
                "max_price":    float(r["max_price"]),
                "total_volume": float(r["total_volume"]),
                "vwap":         float(r["vwap"]),
            } for r in rows
        ],
    }


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    q = await broadcaster.subscribe()
    try:
        # optional symbol filter via first client message {"symbols":[...]}
        symbols_filter: set[str] | None = None
        try:
            first = await asyncio.wait_for(ws.receive_text(), timeout=0.5)
            parsed = orjson.loads(first)
            if isinstance(parsed, dict) and "symbols" in parsed:
                s = parsed["symbols"]
                if isinstance(s, list) and s:
                    symbols_filter = {str(x) for x in s}
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass

        while True:
            msg = await q.get()
            if symbols_filter is not None:
                parsed = orjson.loads(msg)
                sym = (parsed.get("data") or {}).get("symbol")
                if sym not in symbols_filter:
                    continue
            await ws.send_bytes(msg)
    except WebSocketDisconnect:
        pass
    finally:
        await broadcaster.unsubscribe(q)
