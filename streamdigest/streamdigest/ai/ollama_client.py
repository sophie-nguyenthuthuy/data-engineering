"""Thin Ollama HTTP client using the /api/chat JSON-mode endpoint.

Chosen over the `ollama` package to keep deps light and to pin the exact
request shape — JSON-mode responses are strict enough that we can parse
without tool-calling round-trips.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from streamdigest.config import settings


class OllamaError(RuntimeError):
    pass


class OllamaClient:
    def __init__(self, host: str | None = None, model: str | None = None, timeout: float = 120.0):
        self.host = (host or settings.ollama_host).rstrip("/")
        self.model = model or settings.ollama_model
        self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> OllamaClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def chat_json(self, system: str, user: str, *, temperature: float = 0.2) -> dict[str, Any]:
        """Call /api/chat with format=json and return the parsed JSON object."""
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "format": "json",
            "stream": False,
            "options": {"temperature": temperature},
        }
        try:
            resp = self._client.post(f"{self.host}/api/chat", json=payload)
        except httpx.HTTPError as e:
            raise OllamaError(f"Ollama request failed: {e}") from e

        if resp.status_code != 200:
            raise OllamaError(f"Ollama returned {resp.status_code}: {resp.text[:300]}")

        body = resp.json()
        content = (body.get("message") or {}).get("content", "")
        if not content:
            raise OllamaError(f"Ollama returned empty content: {body}")

        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            raise OllamaError(f"Ollama returned non-JSON: {content[:300]}") from e

    def ping(self) -> bool:
        try:
            r = self._client.get(f"{self.host}/api/tags", timeout=5.0)
            return r.status_code == 200
        except httpx.HTTPError:
            return False
