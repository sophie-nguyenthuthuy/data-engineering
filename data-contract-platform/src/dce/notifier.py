"""Dispatch consumer notifications via webhooks or stdout."""

from __future__ import annotations

import json
import logging
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


class WebhookNotifier:
    """POST structured JSON payloads to one or more webhook URLs."""

    def __init__(self, webhook_urls: list[str], *, timeout: int = 10):
        self.webhook_urls = webhook_urls
        self.timeout = timeout

    def send(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Send *payload* to all configured webhooks. Returns per-URL results."""
        results = []
        body = json.dumps(payload, default=str).encode()
        for url in self.webhook_urls:
            try:
                req = urllib.request.Request(
                    url,
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    status = resp.status
                    results.append({"url": url, "status": status, "ok": status < 400})
                    logger.info("Notified %s → HTTP %d", url, status)
            except Exception as exc:
                results.append({"url": url, "status": None, "ok": False, "error": str(exc)})
                logger.warning("Failed to notify %s: %s", url, exc)
        return results


class StdoutNotifier:
    """Print notification payloads to stdout (useful for CI logs)."""

    def send(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        print(json.dumps(payload, indent=2, default=str))
        return [{"ok": True}]


def build_notifier(
    webhook_urls: list[str] | None = None,
    *,
    stdout_fallback: bool = True,
) -> WebhookNotifier | StdoutNotifier:
    if webhook_urls:
        return WebhookNotifier(webhook_urls)
    if stdout_fallback:
        return StdoutNotifier()
    raise ValueError("No notification channel configured")
