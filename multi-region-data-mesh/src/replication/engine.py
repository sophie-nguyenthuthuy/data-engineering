"""
Replication engine — runs as a background asyncio task.

Pull model: every MESH_REPLICATION_INTERVAL_SECONDS this node fetches
all records from each peer that were modified since the last successful
sync, applies conflict resolution, and stores the winners locally.

Lag = (local wall_time NOW) - (remote record's wall_time).
"""
import asyncio
import logging
import time
from typing import Optional

import httpx

from src.config import settings
from src.models import AccountRecord, ReplicationPayload
from src.replication.strategies import resolve_conflict

log = logging.getLogger("mesh.replication")


class PeerState:
    def __init__(self, url: str):
        self.url = url
        self.last_success_at: Optional[float] = None
        self.last_lag_seconds: Optional[float] = None
        self.reachable: bool = False
        # Track the wall_time high-water mark per peer so we only pull deltas
        self._last_sync_since: float = 0.0


class ReplicationEngine:
    def __init__(self, store, region_id: str, conflict_strategy: str):
        self.store = store
        self.region_id = region_id
        self.conflict_strategy = conflict_strategy
        self.peers: dict[str, PeerState] = {
            url: PeerState(url) for url in settings.peer_url_list
        }
        self._task: Optional[asyncio.Task] = None
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    async def start(self):
        self._client = httpx.AsyncClient(timeout=5.0)
        self._task = asyncio.create_task(self._loop(), name="replication-loop")
        log.info("Replication engine started for region=%s peers=%s strategy=%s",
                 self.region_id, list(self.peers), self.conflict_strategy)

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------ #
    #  Main loop                                                           #
    # ------------------------------------------------------------------ #

    async def _loop(self):
        while True:
            await asyncio.sleep(settings.replication_interval_seconds)
            for peer in self.peers.values():
                try:
                    await self._sync_from_peer(peer)
                except Exception as exc:
                    peer.reachable = False
                    log.warning("Replication from %s failed: %s", peer.url, exc)

    # ------------------------------------------------------------------ #
    #  Pull from one peer                                                  #
    # ------------------------------------------------------------------ #

    async def _sync_from_peer(self, peer: PeerState):
        since = peer._last_sync_since
        url = f"{peer.url}/internal/records?since={since}"
        resp = await self._client.get(url)
        resp.raise_for_status()

        payload = ReplicationPayload(**resp.json())
        records = payload.records
        now = time.time()

        if not records:
            peer.reachable = True
            peer.last_success_at = now
            return

        # Compute lag as max difference between now and each record's wall_time
        lag = max(now - r.wall_time for r in records)
        peer.last_lag_seconds = lag
        peer.reachable = True
        peer.last_success_at = now

        applied = 0
        for remote_rec in records:
            applied += await self._apply_record(remote_rec)

        # Advance high-water mark
        peer._last_sync_since = max(r.wall_time for r in records)

        await self.store.log_replication(
            direction="in",
            peer_url=peer.url,
            record_count=applied,
            lag_seconds=lag,
        )
        log.debug("Pulled %d records from %s  lag=%.3fs", applied, peer.url, lag)

    # ------------------------------------------------------------------ #
    #  Apply one incoming record                                          #
    # ------------------------------------------------------------------ #

    async def _apply_record(self, remote: AccountRecord) -> int:
        """
        Returns 1 if the record was stored (new or updated), 0 if skipped.
        """
        local = await self.store.get_account(remote.account_id)

        if local is None:
            # Brand-new to this region — just store it
            await self.store.upsert_account(remote)
            return 1

        local_vc = local.vector_clock
        remote_vc = remote.vector_clock

        if local_vc.dominates(remote_vc):
            # Local is strictly ahead — remote is stale, skip
            return 0

        if remote_vc.dominates(local_vc):
            # Remote is strictly ahead — simple fast-forward
            await self.store.upsert_account(remote)
            return 1

        # Concurrent writes → conflict resolution
        winner, evt = resolve_conflict(
            self.conflict_strategy, local, remote, self.region_id
        )
        await self.store.upsert_account(winner)
        await self.store.log_conflict(evt)
        log.info("Conflict on %s resolved via %s → %s",
                 remote.account_id, self.conflict_strategy, evt.resolution)
        return 1

    # ------------------------------------------------------------------ #
    #  Push helper (called after every local write)                       #
    # ------------------------------------------------------------------ #

    async def push_to_peers(self, record: AccountRecord):
        """Fire-and-forget push so peers can converge faster."""
        payload = ReplicationPayload(
            source_region=self.region_id,
            records=[record],
        )
        for peer in self.peers.values():
            try:
                resp = await self._client.post(
                    f"{peer.url}/internal/records",
                    content=payload.model_dump_json(),
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
                await self.store.log_replication(
                    direction="out",
                    peer_url=peer.url,
                    record_count=1,
                )
            except Exception as exc:
                log.warning("Push to %s failed: %s", peer.url, exc)
