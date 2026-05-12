"""
Finnhub trades producer.

Two modes:
  - LIVE:  if FINNHUB_API_KEY is set, subscribes to wss://ws.finnhub.io for SYMBOLS.
  - DEMO:  if no key, synthesises a correlated random-walk tick stream so the
           full pipeline is runnable out-of-the-box.

Emits canonical JSON to Kafka: { ts_ms, symbol, price, volume, source }
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import signal
import time
from typing import Iterable

import orjson
import websockets
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

log = logging.getLogger("producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TRADES_TOPIC = os.environ["TRADES_TOPIC"]
SYMBOLS = [s.strip() for s in os.environ.get("SYMBOLS", "").split(",") if s.strip()]
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()

FINNHUB_WS_URL = "wss://ws.finnhub.io"


async def build_producer() -> AIOKafkaProducer:
    backoff = 1.0
    while True:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: orjson.dumps(v),
            key_serializer=lambda k: k.encode() if isinstance(k, str) else k,
            linger_ms=50,
            acks="all",
        )
        try:
            await producer.start()
            log.info("kafka producer connected to %s", KAFKA_BOOTSTRAP)
            return producer
        except KafkaConnectionError as e:
            log.warning("kafka not ready (%s), retrying in %.1fs", e, backoff)
            await producer.stop()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 15.0)


async def send_trade(producer: AIOKafkaProducer, symbol: str, price: float, volume: float, ts_ms: int, source: str) -> None:
    payload = {"ts_ms": ts_ms, "symbol": symbol, "price": price, "volume": volume, "source": source}
    await producer.send_and_wait(TRADES_TOPIC, value=payload, key=symbol)


async def run_live(producer: AIOKafkaProducer, symbols: Iterable[str]) -> None:
    url = f"{FINNHUB_WS_URL}?token={FINNHUB_API_KEY}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                for sym in symbols:
                    await ws.send(orjson.dumps({"type": "subscribe", "symbol": sym}).decode())
                log.info("subscribed to %d symbols via finnhub ws", len(list(symbols)))
                async for raw in ws:
                    msg = orjson.loads(raw)
                    if msg.get("type") != "trade":
                        continue
                    for t in msg.get("data") or []:
                        await send_trade(
                            producer,
                            symbol=t["s"],
                            price=float(t["p"]),
                            volume=float(t.get("v", 0) or 0),
                            ts_ms=int(t["t"]),
                            source="finnhub",
                        )
        except Exception as e:
            log.exception("live ws error, reconnecting in 3s: %s", e)
            await asyncio.sleep(3)


class DemoState:
    """Geometric random walk with symbol-specific drift and volatility."""

    PROFILES = {
        "BINANCE:BTCUSDT": (67000.0, 0.0015),
        "BINANCE:ETHUSDT": (3500.0, 0.0018),
        "AAPL":            (185.0,  0.0008),
        "TSLA":            (250.0,  0.0022),
        "NVDA":            (880.0,  0.0025),
    }

    def __init__(self, symbols: Iterable[str]):
        self.state: dict[str, tuple[float, float]] = {}
        for s in symbols:
            start, vol = self.PROFILES.get(s, (100.0, 0.0015))
            self.state[s] = (start, vol)

    def tick(self, symbol: str) -> tuple[float, float]:
        price, vol = self.state[symbol]
        # Lognormal step: price *= exp(N(0, vol))
        price = price * math.exp(random.gauss(0.0, vol))
        # Very rare shocks
        if random.random() < 0.001:
            price *= random.choice([0.995, 1.005])
        self.state[symbol] = (price, vol)
        size = round(abs(random.gauss(0.0, 1.0)) * 5 + 0.1, 4)
        return price, size


async def run_demo(producer: AIOKafkaProducer, symbols: list[str]) -> None:
    log.info("DEMO mode: synthesising ticks for %s", ", ".join(symbols))
    state = DemoState(symbols)
    # Stagger each symbol's cadence so the chart looks alive but not synthetic-uniform.
    async def emit(sym: str):
        base_interval = random.uniform(0.15, 0.45)
        while True:
            price, volume = state.tick(sym)
            await send_trade(
                producer,
                symbol=sym,
                price=round(price, 4),
                volume=volume,
                ts_ms=int(time.time() * 1000),
                source="demo",
            )
            await asyncio.sleep(base_interval * random.uniform(0.6, 1.4))

    await asyncio.gather(*(emit(s) for s in symbols))


async def main() -> None:
    if not SYMBOLS:
        raise SystemExit("SYMBOLS env var is empty")

    producer = await build_producer()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        if FINNHUB_API_KEY:
            task = asyncio.create_task(run_live(producer, SYMBOLS))
        else:
            log.warning("FINNHUB_API_KEY not set; running DEMO mode")
            task = asyncio.create_task(run_demo(producer, SYMBOLS))
        stop_task = asyncio.create_task(stop_event.wait())
        done, pending = await asyncio.wait({task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        for p in pending:
            p.cancel()
    finally:
        await producer.stop()
        log.info("producer stopped")


if __name__ == "__main__":
    asyncio.run(main())
