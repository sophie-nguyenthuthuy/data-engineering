"""
Feature store Python client — wraps the serving API for ML model inference.
Designed for <10ms round-trip on localhost / same-VPC deployments.
"""
from __future__ import annotations

import time
from typing import Any

import httpx


class FeatureStoreClient:
    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        timeout_ms: float = 10.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._http = httpx.Client(
            base_url=self._base,
            timeout=timeout_ms / 1000,
            http2=True,
        )

    def get(self, group: str, entity_id: str) -> dict[str, Any] | None:
        resp = self._http.get(f"/features/{group}/{entity_id}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()["features"]

    def get_batch(
        self, requests: list[tuple[str, str]]
    ) -> list[dict[str, Any] | None]:
        payload = {"requests": [{"group": g, "entity_id": e} for g, e in requests]}
        resp = self._http.post("/features/batch", json=payload)
        resp.raise_for_status()
        return [r["features"] for r in resp.json()["results"]]

    def write(
        self,
        group: str,
        entity_id: str,
        features: dict[str, Any],
        ttl_seconds: int = 86400,
    ) -> None:
        resp = self._http.post(
            f"/features/{group}/{entity_id}",
            json={"entity_id": entity_id, "features": features, "ttl_seconds": ttl_seconds},
        )
        resp.raise_for_status()

    def health(self) -> dict:
        return self._http.get("/health").json()

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> FeatureStoreClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class AsyncFeatureStoreClient:
    """Async client for use inside async ML inference services."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        timeout_ms: float = 10.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._http = httpx.AsyncClient(
            base_url=self._base,
            timeout=timeout_ms / 1000,
            http2=True,
        )

    async def get(self, group: str, entity_id: str) -> dict[str, Any] | None:
        resp = await self._http.get(f"/features/{group}/{entity_id}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()["features"]

    async def get_batch(
        self, requests: list[tuple[str, str]]
    ) -> list[dict[str, Any] | None]:
        payload = {"requests": [{"group": g, "entity_id": e} for g, e in requests]}
        resp = await self._http.post("/features/batch", json=payload)
        resp.raise_for_status()
        return [r["features"] for r in resp.json()["results"]]

    async def close(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> AsyncFeatureStoreClient:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()
