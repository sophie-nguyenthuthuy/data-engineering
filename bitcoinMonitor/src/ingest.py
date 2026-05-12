"""Poller: CoinGecko -> SQLite, every POLL_SECONDS.

Designed to be the simplest viable real-time ingester:
  - one process, no message broker
  - idempotent writes via INSERT OR REPLACE
  - exponential backoff on API errors
"""
from __future__ import annotations

import asyncio
import logging
import time

import httpx

from .config import settings
from .db import init_db, upsert_prices

log = logging.getLogger("ingest")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"


async def fetch_once(client: httpx.AsyncClient) -> list[tuple[str, int, float, str]]:
    params = {
        "ids": ",".join(settings.assets),
        "vs_currencies": settings.vs_currency,
    }
    r = await client.get(COINGECKO_URL, params=params, timeout=10.0)
    r.raise_for_status()
    payload = r.json()
    now = int(time.time())
    rows: list[tuple[str, int, float, str]] = []
    for asset in settings.assets:
        node = payload.get(asset)
        if not node:
            continue
        price = node.get(settings.vs_currency)
        if price is None:
            continue
        rows.append((asset, now, float(price), settings.vs_currency))
    return rows


async def run() -> None:
    init_db()
    backoff = settings.poll_seconds
    async with httpx.AsyncClient() as client:
        while True:
            try:
                rows = await fetch_once(client)
                n = upsert_prices(rows)
                log.info("ingested %d rows (assets=%s)", n, ",".join(settings.assets))
                backoff = settings.poll_seconds
            except httpx.HTTPError as e:
                backoff = min(backoff * 2, 300)
                log.warning("fetch failed: %s — sleeping %ds", e, backoff)
            except Exception as e:  # noqa: BLE001
                backoff = min(backoff * 2, 300)
                log.exception("unexpected error — sleeping %ds: %s", backoff, e)
            await asyncio.sleep(backoff)


if __name__ == "__main__":
    asyncio.run(run())
