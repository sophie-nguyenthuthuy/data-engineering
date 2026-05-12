"""Async WARC record fetcher using HTTP Range requests.

The original project downloaded whole WARC files (~1 GB each) onto Spark workers.
We use Range requests to pull just the bytes we need from data.commoncrawl.org,
cutting bandwidth by 3-4 orders of magnitude.
"""

from __future__ import annotations

import asyncio
import gzip
import io
from collections.abc import AsyncIterator
from dataclasses import dataclass

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from warcio.archiveiterator import ArchiveIterator

from .config import settings
from .ingest import IndexRow
from .logging import get_logger

log = get_logger(__name__)


@dataclass(slots=True)
class FetchedRecord:
    url: str
    fetch_time: str
    html: str


async def _fetch_range(
    client: httpx.AsyncClient, row: IndexRow
) -> bytes:
    url = f"{settings.cc_s3_endpoint}/{row.warc_filename}"
    start = row.warc_record_offset
    end = start + row.warc_record_length - 1
    headers = {"Range": f"bytes={start}-{end}", "User-Agent": settings.user_agent}

    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
        reraise=True,
    ):
        with attempt:
            resp = await client.get(url, headers=headers, timeout=settings.fetch_timeout)
            resp.raise_for_status()
            return resp.content
    return b""  # unreachable; satisfies type checker


def _decode_record(raw: bytes, url: str, fetch_time: str) -> FetchedRecord | None:
    # Record bytes are gzipped; warcio handles a single-record stream fine.
    try:
        stream = io.BytesIO(raw)
        for record in ArchiveIterator(stream):
            if record.rec_type != "response":
                continue
            payload = record.content_stream().read()
            # HTML may be declared in multiple encodings; best-effort decode.
            for enc in ("utf-8", "latin-1"):
                try:
                    return FetchedRecord(url=url, fetch_time=fetch_time, html=payload.decode(enc))
                except UnicodeDecodeError:
                    continue
        return None
    except (OSError, gzip.BadGzipFile) as exc:
        log.warning("fetch.decode_failed", url=url, error=str(exc))
        return None


async def fetch_records(rows: list[IndexRow]) -> AsyncIterator[FetchedRecord]:
    """Fetch all ``rows`` concurrently, yielding decoded HTML as it arrives."""
    sem = asyncio.Semaphore(settings.fetch_concurrency)
    limits = httpx.Limits(
        max_connections=settings.fetch_concurrency * 2,
        max_keepalive_connections=settings.fetch_concurrency,
    )

    async with httpx.AsyncClient(limits=limits, http2=True) as client:
        async def _one(row: IndexRow) -> FetchedRecord | None:
            async with sem:
                try:
                    raw = await _fetch_range(client, row)
                except httpx.HTTPError as exc:
                    log.warning("fetch.failed", url=row.url, error=str(exc))
                    return None
                return _decode_record(raw, row.url, row.fetch_time)

        tasks = [asyncio.create_task(_one(r)) for r in rows]
        for coro in asyncio.as_completed(tasks):
            rec = await coro
            if rec is not None:
                yield rec
