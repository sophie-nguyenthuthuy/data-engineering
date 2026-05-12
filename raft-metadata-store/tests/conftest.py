"""
Shared test fixtures: in-process multi-node Raft cluster.

Spins up N RaftNode instances with real Raft logic but in-memory
transport (monkeypatched RPC) so tests run fast without real sockets.
"""

import asyncio
import os
import shutil
import tempfile
from typing import Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from raft.node import RaftNode, RaftState
from raft.rpc import RaftRPC
from store.kv_store import KVStore


class InProcessRPC:
    """
    RPC transport that delivers messages directly to in-process nodes.
    Allows injecting network partitions and delays.
    """

    # node_id → RaftNode (populated after cluster creation)
    _registry: Dict[str, "RaftNode"] = {}
    # frozenset({a, b}) → True means partitioned
    _partitions: set = set()
    # node_id → delay seconds
    _delays: Dict[str, float] = {}

    @classmethod
    def register(cls, node_id: str, node: "RaftNode") -> None:
        cls._registry[node_id] = node

    @classmethod
    def partition(cls, a: str, b: str) -> None:
        cls._partitions.add(frozenset([a, b]))

    @classmethod
    def heal(cls, a: str, b: str) -> None:
        cls._partitions.discard(frozenset([a, b]))

    @classmethod
    def heal_all(cls) -> None:
        cls._partitions.clear()
        cls._delays.clear()

    @classmethod
    def set_delay(cls, node_id: str, delay: float) -> None:
        cls._delays[node_id] = delay

    @classmethod
    def _is_partitioned(cls, src: str, dst_addr: str) -> bool:
        # Find dst node_id by addr
        for nid, node in cls._registry.items():
            if nid == src:
                continue
            own_addr = getattr(node, "_listen_addr", None)
            if own_addr == dst_addr or True:
                # Match by node_id embedded in addr
                if dst_addr.endswith(nid) or nid in dst_addr:
                    return frozenset([src, nid]) in cls._partitions
        return False

    def __init__(self, node_id: str):
        self.node_id = node_id

    async def request_vote(self, peer_addr, term, candidate_id, last_log_index, last_log_term):
        peer = self._get_peer(peer_addr)
        if peer is None:
            return None
        return await peer.handle_request_vote({
            "term": term, "candidate_id": candidate_id,
            "last_log_index": last_log_index, "last_log_term": last_log_term,
        })

    async def append_entries(self, peer_addr, term, leader_id, prev_log_index,
                              prev_log_term, entries, leader_commit):
        peer = self._get_peer(peer_addr)
        if peer is None:
            return None
        delay = self._delays.get(leader_id, 0)
        if delay:
            await asyncio.sleep(delay)
        return await peer.handle_append_entries({
            "term": term, "leader_id": leader_id,
            "prev_log_index": prev_log_index, "prev_log_term": prev_log_term,
            "entries": entries, "leader_commit": leader_commit,
        })

    async def install_snapshot(self, peer_addr, term, leader_id, last_included_index,
                                last_included_term, data, cluster_config):
        peer = self._get_peer(peer_addr)
        if peer is None:
            return None
        return await peer.handle_install_snapshot({
            "term": term, "leader_id": leader_id,
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
            "data": data, "cluster_config": cluster_config,
        })

    async def close(self):
        pass

    def _get_peer(self, peer_addr: str) -> Optional["RaftNode"]:
        for nid, node in self._registry.items():
            if nid == self.node_id:
                continue
            if peer_addr == nid or peer_addr.endswith(f":{nid}") or nid in peer_addr:
                if frozenset([self.node_id, nid]) in self._partitions:
                    return None
                return node
        return None


def make_cluster(n: int, tmp_dir: str) -> Tuple[List[RaftNode], List[KVStore]]:
    InProcessRPC._registry.clear()
    InProcessRPC._partitions.clear()
    InProcessRPC._delays.clear()

    node_ids = [f"node{i+1}" for i in range(n)]
    # Use node_id as "addr" for in-process routing
    peer_map = {nid: nid for nid in node_ids}

    stores = []
    nodes = []
    for nid in node_ids:
        peers = {p: p for p in node_ids if p != nid}
        kv = KVStore()
        stores.append(kv)
        node = RaftNode(
            node_id=nid,
            peers=peers,
            state_machine_apply=kv.apply,
            state_machine_snapshot=kv.snapshot,
            state_machine_restore=kv.restore,
            data_dir=os.path.join(tmp_dir, nid),
        )
        node.rpc = InProcessRPC(nid)
        nodes.append(node)
        InProcessRPC.register(nid, node)

    return nodes, stores


async def start_cluster(nodes: List[RaftNode]) -> None:
    for node in nodes:
        await node.start()


async def stop_cluster(nodes: List[RaftNode]) -> None:
    for node in nodes:
        await node.stop()


async def wait_for_leader(
    nodes: List[RaftNode], timeout: float = 5.0
) -> Optional[RaftNode]:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        for node in nodes:
            if node.state == RaftState.LEADER:
                return node
        await asyncio.sleep(0.05)
    return None


@pytest_asyncio.fixture
async def three_node_cluster(tmp_path):
    nodes, stores = make_cluster(3, str(tmp_path))
    await start_cluster(nodes)
    yield nodes, stores
    await stop_cluster(nodes)
    InProcessRPC.heal_all()


@pytest_asyncio.fixture
async def five_node_cluster(tmp_path):
    nodes, stores = make_cluster(5, str(tmp_path))
    await start_cluster(nodes)
    yield nodes, stores
    await stop_cluster(nodes)
    InProcessRPC.heal_all()
