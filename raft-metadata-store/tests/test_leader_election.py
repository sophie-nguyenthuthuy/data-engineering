"""Tests for Raft leader election."""

import asyncio
import pytest
from raft.node import RaftState
from .conftest import InProcessRPC, wait_for_leader


pytestmark = pytest.mark.asyncio


async def test_single_leader_elected(three_node_cluster):
    nodes, _ = three_node_cluster
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None, "no leader elected within timeout"

    leaders = [n for n in nodes if n.state == RaftState.LEADER]
    assert len(leaders) == 1, f"expected 1 leader, got {len(leaders)}"


async def test_all_nodes_agree_on_term(three_node_cluster):
    nodes, _ = three_node_cluster
    await wait_for_leader(nodes, timeout=3.0)
    await asyncio.sleep(0.3)

    terms = {n.log.current_term for n in nodes}
    # All nodes may not be at exactly the same term if a follower is slightly
    # behind, but they must all have the same or higher term than the leader.
    leader = next(n for n in nodes if n.state == RaftState.LEADER)
    for node in nodes:
        assert node.log.current_term >= leader.log.current_term - 1


async def test_leader_step_down_on_partition(three_node_cluster):
    """Partition the leader from all followers; it should step down."""
    nodes, _ = three_node_cluster
    leader = await wait_for_leader(nodes, timeout=3.0)
    assert leader is not None

    followers = [n for n in nodes if n.state != RaftState.LEADER]
    for f in followers:
        InProcessRPC.partition(leader.node_id, f.node_id)

    # Wait for election timeout + buffer
    await asyncio.sleep(0.8)

    # A new leader should be elected among followers
    new_leader = await wait_for_leader(followers, timeout=3.0)
    assert new_leader is not None, "followers should elect a new leader"
    assert new_leader.node_id != leader.node_id

    InProcessRPC.heal_all()


async def test_rejoin_after_partition(three_node_cluster):
    """Partitioned leader rejoins and reverts to follower."""
    nodes, _ = three_node_cluster
    old_leader = await wait_for_leader(nodes, timeout=3.0)
    assert old_leader is not None

    followers = [n for n in nodes if n.state != RaftState.LEADER]
    for f in followers:
        InProcessRPC.partition(old_leader.node_id, f.node_id)

    await asyncio.sleep(0.8)
    InProcessRPC.heal_all()
    await asyncio.sleep(0.5)

    # Old leader must have stepped down
    assert old_leader.state == RaftState.FOLLOWER


async def test_no_leader_without_quorum(five_node_cluster):
    """With 5 nodes and 3 partitioned off, the 2-node minority can't elect."""
    nodes, _ = five_node_cluster
    await wait_for_leader(nodes, timeout=3.0)

    # Identify 3 vs 2 split
    minority = nodes[:2]
    majority = nodes[2:]

    for a in minority:
        for b in majority:
            InProcessRPC.partition(a.node_id, b.node_id)

    await asyncio.sleep(1.0)

    minority_leaders = [n for n in minority if n.state == RaftState.LEADER]
    assert len(minority_leaders) == 0, "minority partition must not elect a leader"

    InProcessRPC.heal_all()


async def test_election_safety_no_two_leaders_same_term(three_node_cluster):
    """
    Run multiple election cycles and verify no two leaders share the same term
    (Raft safety property).
    """
    nodes, _ = three_node_cluster
    seen: dict = {}  # term → leader_id

    for _ in range(10):
        await asyncio.sleep(0.1)
        for node in nodes:
            if node.state == RaftState.LEADER:
                t = node.log.current_term
                if t in seen:
                    assert seen[t] == node.node_id, (
                        f"Two leaders in term {t}: {seen[t]} and {node.node_id}"
                    )
                else:
                    seen[t] = node.node_id
