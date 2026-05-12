"""HTTP-based RPC transport for Raft inter-node communication."""

import asyncio
import json
import logging
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)

RPC_TIMEOUT = 0.5  # seconds


class RaftRPC:
    """Async HTTP client for Raft RPC calls."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._session: Optional[aiohttp.ClientSession] = None

    async def _session_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=RPC_TIMEOUT)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def request_vote(
        self,
        peer_addr: str,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ) -> Optional[Dict[str, Any]]:
        payload = {
            "term": term,
            "candidate_id": candidate_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
        }
        return await self._post(peer_addr, "/raft/request_vote", payload)

    async def append_entries(
        self,
        peer_addr: str,
        term: int,
        leader_id: str,
        prev_log_index: int,
        prev_log_term: int,
        entries: list,
        leader_commit: int,
    ) -> Optional[Dict[str, Any]]:
        payload = {
            "term": term,
            "leader_id": leader_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries,
            "leader_commit": leader_commit,
        }
        return await self._post(peer_addr, "/raft/append_entries", payload)

    async def install_snapshot(
        self,
        peer_addr: str,
        term: int,
        leader_id: str,
        last_included_index: int,
        last_included_term: int,
        data: Dict[str, Any],
        cluster_config: list,
    ) -> Optional[Dict[str, Any]]:
        payload = {
            "term": term,
            "leader_id": leader_id,
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
            "data": data,
            "cluster_config": cluster_config,
        }
        return await self._post(peer_addr, "/raft/install_snapshot", payload)

    async def _post(
        self, addr: str, path: str, payload: dict
    ) -> Optional[Dict[str, Any]]:
        url = f"http://{addr}{path}"
        try:
            session = await self._session_get()
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.debug("RPC to %s%s failed: %s", addr, path, exc)
        return None
