"""Tests for cluster membership changes."""

import asyncio
import os
import pytest
from raft.node import RaftState
from store.kv_store import KVStore
from .conftest import InProcessRPC, make_cluster, start_cluster, stop_cluster, wait_for_leader


pytestmark = pytest.mark.asyncio


async def test_add_peer(tmp_path):
    """Add a 4th node to a 3-node cluster."""
    nodes, stores = make_cluster(3, str(tmp_path))
    await start_cluster(nodes)
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    # Write something before adding peer
    await leader.submit({"op": "put", "key": "before_join", "value": "yes"})

    # Create 4th node
    new_kv = KVStore()
    new_node = __import__("raft.node", fromlist=["RaftNode"]).RaftNode(
        node_id="node4",
        peers={n.node_id: n.node_id for n in nodes},
        state_machine_apply=new_kv.apply,
        state_machine_snapshot=new_kv.snapshot,
        state_machine_restore=new_kv.restore,
        data_dir=str(tmp_path / "node4"),
    )
    from tests.conftest import InProcessRPC
    new_node.rpc = InProcessRPC("node4")
    InProcessRPC.register("node4", new_node)
    await new_node.start()

    try:
        await leader.add_peer("node4", "node4")
        await asyncio.sleep(0.5)

        assert "node4" in leader.peers
    finally:
        await new_node.stop()
        await stop_cluster(nodes)
        InProcessRPC.heal_all()


async def test_remove_peer(tmp_path):
    """Remove a follower from a 3-node cluster, leaving 2-node majority."""
    nodes, stores = make_cluster(3, str(tmp_path))
    await start_cluster(nodes)
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    follower = next(n for n in nodes if n.state != RaftState.LEADER)
    await leader.remove_peer(follower.node_id)
    await asyncio.sleep(0.3)

    assert follower.node_id not in leader.peers

    # Cluster should still function
    result = await leader.submit({"op": "put", "key": "after_remove", "value": 42})
    assert result["ok"] is True

    await stop_cluster(nodes)
    InProcessRPC.heal_all()
