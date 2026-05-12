"""Tests for log replication and consistency."""

import asyncio
import pytest
from raft.node import RaftState
from .conftest import InProcessRPC, wait_for_leader


pytestmark = pytest.mark.asyncio


async def test_write_and_read(three_node_cluster):
    nodes, stores = three_node_cluster
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    result = await leader.submit({"op": "put", "key": "foo", "value": "bar"})
    assert result["ok"] is True

    await asyncio.sleep(0.1)  # let replication propagate

    for store in stores:
        vv = await store.get("foo")
        assert vv is not None and vv.value == "bar", (
            f"follower store has wrong value: {vv}"
        )


async def test_multiple_writes_ordered(three_node_cluster):
    nodes, stores = three_node_cluster
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    for i in range(10):
        await leader.submit({"op": "put", "key": f"k{i}", "value": i * 10})

    await asyncio.sleep(0.3)

    for store in stores:
        for i in range(10):
            vv = await store.get(f"k{i}")
            assert vv is not None and vv.value == i * 10


async def test_commit_index_advances(three_node_cluster):
    nodes, _ = three_node_cluster
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    for i in range(5):
        await leader.submit({"op": "put", "key": f"x{i}", "value": i})

    await asyncio.sleep(0.3)
    for node in nodes:
        assert node.commit_index >= 5, (
            f"node {node.node_id} commit_index={node.commit_index}"
        )


async def test_replication_after_partition_heal(three_node_cluster):
    """Writes during a follower partition are replicated after heal."""
    nodes, stores = three_node_cluster
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    isolated = next(n for n in nodes if n.state != RaftState.LEADER)
    InProcessRPC.partition(leader.node_id, isolated.node_id)

    # Write while partitioned
    for i in range(5):
        await leader.submit({"op": "put", "key": f"p{i}", "value": i})

    InProcessRPC.heal_all()
    await asyncio.sleep(0.5)  # give time to catch up

    isolated_store = stores[nodes.index(isolated)]
    for i in range(5):
        vv = await isolated_store.get(f"p{i}")
        assert vv is not None, f"p{i} not replicated to rejoined node"
        assert vv.value == i


async def test_stale_leader_cannot_commit(three_node_cluster):
    """
    A partitioned leader cannot commit new entries without a quorum.
    """
    nodes, _ = three_node_cluster
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    followers = [n for n in nodes if n.state != RaftState.LEADER]
    for f in followers:
        InProcessRPC.partition(leader.node_id, f.node_id)

    await asyncio.sleep(0.5)

    with pytest.raises(Exception):
        await asyncio.wait_for(
            leader.submit({"op": "put", "key": "isolated", "value": "x"}),
            timeout=2.0,
        )

    InProcessRPC.heal_all()


async def test_delete_operation(three_node_cluster):
    nodes, stores = three_node_cluster
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    await leader.submit({"op": "put", "key": "to_delete", "value": "bye"})
    await asyncio.sleep(0.1)
    await leader.submit({"op": "delete", "key": "to_delete"})
    await asyncio.sleep(0.2)

    for store in stores:
        vv = await store.get("to_delete")
        assert vv is None, "key should be deleted on all nodes"


async def test_cas_operation(three_node_cluster):
    nodes, stores = three_node_cluster
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    await leader.submit({"op": "put", "key": "counter", "value": 0})
    await asyncio.sleep(0.05)

    # Successful CAS
    r = await leader.submit(
        {"op": "cas", "key": "counter", "expected": 0, "new_value": 1}
    )
    assert r["ok"] is True

    # Failed CAS (wrong expected)
    r2 = await leader.submit(
        {"op": "cas", "key": "counter", "expected": 0, "new_value": 99}
    )
    assert r2["ok"] is False

    await asyncio.sleep(0.2)
    for store in stores:
        vv = await store.get("counter")
        assert vv is not None and vv.value == 1
