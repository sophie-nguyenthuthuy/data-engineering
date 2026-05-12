"""HTTP/Webhook target — POSTs events to any HTTP endpoint with retry logic."""

from __future__ import annotations

import json
import logging

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from replay.models import Event, HttpTargetConfig
from replay.targets.base import BaseTarget

logger = logging.getLogger(__name__)


class HttpTarget(BaseTarget):
    """
    Sends each event as an HTTP request to a configured endpoint.

    Request body (JSON):
      {
        "topic": "...",
        "partition": 0,
        "offset": 123,
        "key": "...",          # base64 if bytes
        "value": { ... },      # parsed JSON or raw string
        "timestamp": "ISO8601",
        "headers": { ... }
      }

    Retries up to config.max_retries times with exponential back-off.
    """

    def __init__(self, config: HttpTargetConfig) -> None:
        self.config = config
        self._session: aiohttp.ClientSession | None = None

    async def open(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        self._session = aiohttp.ClientSession(
            headers=self.config.headers,
            timeout=timeout,
        )
        logger.info("HTTP target opened: %s %s", self.config.method, self.config.url)

    async def send(self, event: Event) -> None:
        payload = _event_to_dict(event)
        await self._send_with_retry(payload)

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def _send_with_retry(self, payload: dict) -> None:
        @retry(
            stop=stop_after_attempt(self.config.max_retries),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=10),
            retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
            reraise=True,
        )
        async def _do() -> None:
            assert self._session is not None
            async with self._session.request(
                method=self.config.method,
                url=self.config.url,
                json=payload,
            ) as resp:
                if resp.status >= 500:
                    text = await resp.text()
                    raise aiohttp.ClientResponseError(
                        resp.request_info,
                        resp.history,
                        status=resp.status,
                        message=text,
                    )
                if resp.status >= 400:
                    text = await resp.text()
                    logger.warning("HTTP %d for event offset=%s: %s",
                                   resp.status, payload.get("offset"), text)

        await _do()


def _event_to_dict(event: Event) -> dict:
    try:
        value = json.loads(event.value)
    except (json.JSONDecodeError, UnicodeDecodeError):
        value = event.value.decode("utf-8", errors="replace")

    return {
        "topic": event.topic,
        "partition": event.partition,
        "offset": event.offset,
        "key": event.key.decode("utf-8", errors="replace") if event.key else None,
        "value": value,
        "timestamp": event.timestamp.isoformat(),
        "headers": {k: v.decode("utf-8", errors="replace") for k, v in event.headers.items()},
        "_replay": True,
        "_source_path": event.source_path,
    }
