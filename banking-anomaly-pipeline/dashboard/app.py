"""
Dashboard backend — FastAPI serving:
  GET /          → monitoring UI
  GET /stream    → SSE: live transactions
  GET /alerts    → SSE: fraud alerts
  GET /api/stats → JSON snapshot stats
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque, defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TX_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "transactions")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "fraud-alerts")

MAX_RECENT = 200

# Shared in-memory state (single-process; use Redis for multi-replica)
state = {
    "transactions": deque(maxlen=MAX_RECENT),
    "alerts": deque(maxlen=MAX_RECENT),
    "tx_count": 0,
    "alert_count": 0,
    "alerts_by_rule": defaultdict(int),
    "alerts_by_severity": defaultdict(int),
    "recent_amounts": deque(maxlen=50),
    "started_at": datetime.now(timezone.utc).isoformat(),
}

# SSE subscriber queues
tx_subscribers: list[asyncio.Queue] = []
alert_subscribers: list[asyncio.Queue] = []


def _make_consumer(group_id: str, topic: str) -> Consumer:
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    c.subscribe([topic])
    return c


async def _kafka_poller(consumer: Consumer, dest_deque: deque, subs: list,
                        counter_key: str, extra_fn=None):
    loop = asyncio.get_event_loop()
    while True:
        msg = await loop.run_in_executor(None, lambda: consumer.poll(0.1))
        if msg is None or (msg.error() and msg.error().code() == KafkaError._PARTITION_EOF):
            await asyncio.sleep(0.05)
            continue
        if msg.error():
            await asyncio.sleep(0.2)
            continue
        try:
            data = json.loads(msg.value().decode())
            dest_deque.append(data)
            state[counter_key] += 1
            if extra_fn:
                extra_fn(data)
            payload = json.dumps(data)
            dead = []
            for q in subs:
                try:
                    q.put_nowait(payload)
                except asyncio.QueueFull:
                    dead.append(q)
            for q in dead:
                subs.remove(q)
        except Exception:
            pass


def _on_alert(alert: dict):
    for sig in alert.get("signals", []):
        state["alerts_by_rule"][sig["rule"]] += 1
    state["alerts_by_severity"][alert.get("severity", "UNKNOWN")] += 1


def _on_tx(tx: dict):
    state["recent_amounts"].append(tx.get("amount", 0))


@asynccontextmanager
async def lifespan(app: FastAPI):
    tx_consumer = _make_consumer("dashboard-tx", TX_TOPIC)
    alert_consumer = _make_consumer("dashboard-alerts", ALERTS_TOPIC)
    t1 = asyncio.create_task(
        _kafka_poller(tx_consumer, state["transactions"], tx_subscribers, "tx_count", _on_tx)
    )
    t2 = asyncio.create_task(
        _kafka_poller(alert_consumer, state["alerts"], alert_subscribers, "alert_count", _on_alert)
    )
    yield
    t1.cancel()
    t2.cancel()
    tx_consumer.close()
    alert_consumer.close()


app = FastAPI(title="Banking Anomaly Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


async def _sse_generator(subscribers: list, queue: asyncio.Queue):
    subscribers.append(queue)
    try:
        # Send last 10 buffered events on connect
        while True:
            try:
                data = await asyncio.wait_for(queue.get(), timeout=25)
                yield f"data: {data}\n\n"
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
    finally:
        try:
            subscribers.remove(queue)
        except ValueError:
            pass


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html") as f:
        return f.read()


@app.get("/stream")
async def stream_transactions():
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    return StreamingResponse(
        _sse_generator(tx_subscribers, q),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/alerts")
async def stream_alerts():
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    return StreamingResponse(
        _sse_generator(alert_subscribers, q),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/stats")
async def stats():
    amounts = list(state["recent_amounts"])
    avg_amount = sum(amounts) / len(amounts) if amounts else 0
    return {
        "tx_count": state["tx_count"],
        "alert_count": state["alert_count"],
        "alert_rate_pct": round(
            state["alert_count"] / state["tx_count"] * 100, 2
        ) if state["tx_count"] else 0,
        "avg_amount": round(avg_amount, 2),
        "alerts_by_rule": dict(state["alerts_by_rule"]),
        "alerts_by_severity": dict(state["alerts_by_severity"]),
        "started_at": state["started_at"],
        "uptime_sec": round(
            (datetime.now(timezone.utc) - datetime.fromisoformat(state["started_at"])).total_seconds()
        ),
    }
