"""Tests for log compaction and snapshotting."""

import asyncio
import os
import pytest
from raft.node import RaftNode, RaftState, SNAPSHOT_THRESHOLD
from store.kv_store import KVStore
from .conftest import InProcessRPC, make_cluster, start_cluster, stop_cluster, wait_for_leader


pytestmark = pytest.mark.asyncio


async def test_snapshot_taken_after_threshold(tmp_path):
    """After SNAPSHOT_THRESHOLD commits, a snapshot should be present."""
    # Use a tiny threshold for testing
    import raft.node as rn
    orig = rn.SNAPSHOT_THRESHOLD
    rn.SNAPSHOT_THRESHOLD = 10
    try:
        nodes, stores = make_cluster(3, str(tmp_path))
        await start_cluster(nodes)
        leader = await wait_for_leader(nodes, timeout=3.0)
        assert leader is not None

        for i in range(15):
            await leader.submit({"op": "put", "key": f"snap{i}", "value": i})

        await asyncio.sleep(0.5)

        for node in nodes:
            # At least the leader should have snapshotted
            if node.state == RaftState.LEADER:
                assert node.log.snapshot is not None, "leader should have snapshot"
                assert node.log.snapshot.last_included_index >= 10
    finally:
        rn.SNAPSHOT_THRESHOLD = orig
        await stop_cluster(nodes)
        InProcessRPC.heal_all()


async def test_snapshot_restores_state(tmp_path):
    """Nodes restored from snapshot have correct state."""
    import raft.node as rn
    orig = rn.SNAPSHOT_THRESHOLD
    rn.SNAPSHOT_THRESHOLD = 5
    try:
        nodes, stores = make_cluster(3, str(tmp_path))
        await start_cluster(nodes)
        leader = await wait_for_leader(nodes, timeout=3.0)
        assert leader is not None

        # Write enough to trigger snapshot
        for i in range(8):
            await leader.submit({"op": "put", "key": f"r{i}", "value": i * 2})

        await asyncio.sleep(0.5)

        # All stores should have the correct data
        for store in stores:
            for i in range(8):
                vv = await store.get(f"r{i}")
                assert vv is not None and vv.value == i * 2
    finally:
        rn.SNAPSHOT_THRESHOLD = orig
        await stop_cluster(nodes)
        InProcessRPC.heal_all()


async def test_lagging_follower_gets_snapshot(tmp_path):
    """A follower that falls too far behind receives a snapshot."""
    import raft.node as rn
    orig = rn.SNAPSHOT_THRESHOLD
    rn.SNAPSHOT_THRESHOLD = 5
    try:
        nodes, stores = make_cluster(3, str(tmp_path))
        await start_cluster(nodes)
        leader = await wait_for_leader(nodes, timeout=3.0)
        assert leader is not None

        laggard = next(n for n in nodes if n.state != RaftState.LEADER)
        InProcessRPC.partition(leader.node_id, laggard.node_id)

        # Write enough to snapshot while laggard is isolated
        for i in range(8):
            await leader.submit({"op": "put", "key": f"lag{i}", "value": i})

        await asyncio.sleep(0.3)

        # Leader should have snapshot
        assert leader.log.snapshot is not None

        # Heal and allow InstallSnapshot to flow
        InProcessRPC.heal_all()
        await asyncio.sleep(1.0)

        laggard_store = stores[nodes.index(laggard)]
        for i in range(8):
            vv = await laggard_store.get(f"lag{i}")
            assert vv is not None, f"lag{i} missing from laggard after snapshot install"
            assert vv.value == i
    finally:
        rn.SNAPSHOT_THRESHOLD = orig
        await stop_cluster(nodes)
        InProcessRPC.heal_all()
