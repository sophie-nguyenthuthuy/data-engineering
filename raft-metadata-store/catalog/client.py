"""
HTTP client for the catalog API.
Transparently retries on non-leader nodes by following the leader redirect.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)
MAX_REDIRECTS = 5


class CatalogClient:
    def __init__(self, addrs: List[str]):
        self._addrs = list(addrs)
        self._leader_addr: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)
        )
        return self

    async def __aexit__(self, *_):
        if self._session:
            await self._session.close()

    # ── Catalog operations ────────────────────────────────────────────────

    async def create_dataset(self, name: str, **kwargs) -> Dict:
        return await self._post("/catalog/datasets", {"name": name, **kwargs})

    async def get_dataset(self, name: str) -> Dict:
        return await self._get(f"/catalog/datasets/{name}")

    async def list_datasets(self) -> List[Dict]:
        r = await self._get("/catalog/datasets")
        return r.get("datasets", [])

    async def create_table(self, dataset: str, name: str, **kwargs) -> Dict:
        return await self._post(
            f"/catalog/datasets/{dataset}/tables",
            {"name": name, **kwargs},
        )

    async def get_table(self, dataset: str, table: str) -> Dict:
        return await self._get(f"/catalog/datasets/{dataset}/tables/{table}")

    async def list_tables(self, dataset: str) -> List[Dict]:
        r = await self._get(f"/catalog/datasets/{dataset}/tables")
        return r.get("tables", [])

    async def add_lineage(
        self, source: str, target: str, job: str, **kwargs
    ) -> Dict:
        return await self._post(
            "/catalog/lineage",
            {"source": source, "target": target, "job": job, **kwargs},
        )

    async def lineage_upstream(self, table: str) -> List[Dict]:
        r = await self._get(f"/catalog/lineage/upstream/{table}")
        return r.get("lineage", [])

    async def lineage_downstream(self, table: str) -> List[Dict]:
        r = await self._get(f"/catalog/lineage/downstream/{table}")
        return r.get("lineage", [])

    # ── KV passthrough (for linearizability tests) ────────────────────────

    async def kv_put(self, key: str, value: Any, version: Optional[int] = None) -> Dict:
        payload: Dict = {"key": key, "value": value}
        if version is not None:
            payload["version"] = version
        return await self._post("/kv/put", payload)

    async def kv_get(self, key: str) -> Dict:
        return await self._get(f"/kv/get/{key}")

    async def kv_cas(self, key: str, expected: Any, new_value: Any) -> Dict:
        return await self._post(
            "/kv/cas", {"key": key, "expected": expected, "new_value": new_value}
        )

    async def kv_delete(self, key: str) -> Dict:
        return await self._post("/kv/delete", {"key": key})

    async def cluster_status(self) -> Dict:
        return await self._get("/raft/status")

    # ── Private HTTP helpers ──────────────────────────────────────────────

    async def _get(self, path: str) -> Dict:
        return await self._request("GET", path)

    async def _post(self, path: str, body: dict) -> Dict:
        return await self._request("POST", path, body)

    async def _request(
        self, method: str, path: str, body: Optional[dict] = None
    ) -> Dict:
        addrs = (
            [self._leader_addr] + self._addrs
            if self._leader_addr
            else self._addrs
        )
        seen = set()
        for _ in range(MAX_REDIRECTS):
            for addr in addrs:
                if addr in seen:
                    continue
                seen.add(addr)
                url = f"http://{addr}{path}"
                try:
                    if method == "GET":
                        resp_ctx = self._session.get(url)
                    else:
                        resp_ctx = self._session.post(url, json=body)
                    async with resp_ctx as resp:
                        data = await resp.json()
                        if resp.status == 200:
                            self._leader_addr = addr
                            return data
                        if resp.status == 307:
                            leader = data.get("leader_addr")
                            if leader:
                                self._leader_addr = leader
                                addrs = [leader] + self._addrs
                            break
                        logger.warning("HTTP %d from %s%s", resp.status, addr, path)
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logger.debug("request to %s failed: %s", url, e)
                    continue
        raise RuntimeError(f"all nodes unreachable for {method} {path}")
