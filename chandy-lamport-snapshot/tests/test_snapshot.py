"""
Tests for the Chandy-Lamport snapshot protocol.

Verifies:
  - Source nodes (no incoming channels) complete their local snapshot
    immediately after sending markers.
  - Processing nodes complete only after receiving markers on ALL channels.
  - The channel state of the *first* channel to deliver a marker is empty
    (FIFO guarantee).
  - A GlobalSnapshot is assembled once every node has reported.
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import List

import pytest

from chandy_lamport import (
    AggregatorNode,
    Channel,
    DataMessage,
    GlobalSnapshot,
    Marker,
    MergeNode,
    NodeSnapshot,
    Pipeline,
    SinkNode,
    SnapshotCoordinator,
    SourceNode,
    TransformNode,
)


# ── helpers ───────────────────────────────────────────────────────────────────


def make_linear_pipeline(n_msgs: int = 5):
    """
    Source → Transform → Sink (linear, single-input nodes throughout).
    Returns (source, transform, sink, channels).
    """
    src = SourceNode("src", total=n_msgs, interval=0.02)
    tx  = TransformNode("tx",  fn=lambda x: x + 100)
    snk = SinkNode("snk")

    ch_s_t = Channel("src", "tx")
    ch_t_s = Channel("tx",  "snk")

    src.add_out_channel(ch_s_t)
    tx.add_in_channel(ch_s_t)
    tx.add_out_channel(ch_t_s)
    snk.add_in_channel(ch_t_s)

    for n in (src, tx, snk):
        n.setup()

    return src, tx, snk, (ch_s_t, ch_t_s)


# ── unit: SnapshotCoordinator ─────────────────────────────────────────────────


class TestSnapshotCoordinator:
    def test_fires_when_all_nodes_report(self):
        received: List[GlobalSnapshot] = []
        coord = SnapshotCoordinator(
            "snap-1", ["A", "B", "C"], on_complete=received.append
        )
        coord.receive(NodeSnapshot("A", state={"x": 1}, channel_states={}))
        assert len(received) == 0
        coord.receive(NodeSnapshot("B", state={"x": 2}, channel_states={}))
        assert len(received) == 0
        coord.receive(NodeSnapshot("C", state={"x": 3}, channel_states={}))
        assert len(received) == 1
        assert received[0].complete is True

    def test_fires_only_once_on_duplicate_reports(self):
        received: List[GlobalSnapshot] = []
        coord = SnapshotCoordinator(
            "snap-2", ["A", "B"], on_complete=received.append
        )
        for _ in range(3):
            coord.receive(NodeSnapshot("A", {}, {}))
            coord.receive(NodeSnapshot("B", {}, {}))
        assert len(received) == 1

    def test_single_node_fires_immediately(self):
        received: List[GlobalSnapshot] = []
        coord = SnapshotCoordinator("snap-3", ["A"], on_complete=received.append)
        coord.receive(NodeSnapshot("A", {"v": 42}, {}))
        assert len(received) == 1
        assert received[0].node_snapshots["A"].state == {"v": 42}


# ── unit: Node snapshot state machine ─────────────────────────────────────────


class TestNodeSnapshotMachine:
    """Drive the Chandy-Lamport state machine directly without threading."""

    def _make_coord(self, node_ids, snapshots):
        return SnapshotCoordinator(
            "snap-x", node_ids, on_complete=snapshots.append
        )

    def test_source_node_completes_immediately(self):
        snapshots: List[GlobalSnapshot] = []
        src = SourceNode("src", total=5, interval=0.1)
        src.setup()
        coord = self._make_coord(["src"], snapshots)
        src.set_coordinator(coord)
        src.initiate_snapshot("snap-x")
        assert len(snapshots) == 1
        assert snapshots[0].node_snapshots["src"].state["next_seq"] == 1

    def test_processing_node_waits_for_all_channels(self):
        """MergeNode has 2 inputs; snapshot completes only after both markers."""
        snapshots: List[GlobalSnapshot] = []
        merge = MergeNode("merge")
        merge.setup()

        ch1 = Channel("A", "merge")
        ch2 = Channel("B", "merge")
        merge.add_in_channel(ch1)
        merge.add_in_channel(ch2)

        coord = self._make_coord(["merge"], snapshots)
        merge.set_coordinator(coord)

        marker = Marker(snapshot_id="snap-x", initiator_id="external")

        # First marker — should NOT complete yet
        merge._on_marker(marker, ch1)
        assert len(snapshots) == 0

        # Second marker — should complete now
        merge._on_marker(marker, ch2)
        assert len(snapshots) == 1

    def test_first_channel_state_is_empty(self):
        """FIFO: when the first marker arrives, that channel carries no in-transit msgs."""
        snapshots: List[GlobalSnapshot] = []
        merge = MergeNode("merge")
        merge.setup()

        ch1 = Channel("fast", "merge")
        ch2 = Channel("slow", "merge")
        merge.add_in_channel(ch1)
        merge.add_in_channel(ch2)

        coord = self._make_coord(["merge"], snapshots)
        merge.set_coordinator(coord)

        marker = Marker("snap-x", "ext")
        merge._on_marker(marker, ch1)          # first marker on ch1
        # inject data on ch2 (simulates in-transit messages)
        msg = DataMessage(content=99, origin_seq=99)
        ch2.start_recording()                  # already started by _on_marker
        ch2.record_if_needed(msg)
        merge._on_marker(marker, ch2)          # second marker on ch2

        ns = snapshots[0].node_snapshots["merge"]
        assert ns.channel_states[ch1.name] == []          # first channel: empty
        assert len(ns.channel_states[ch2.name]) == 1      # slow channel: 1 in-transit


# ── integration: linear pipeline snapshot ─────────────────────────────────────


class TestLinearPipelineSnapshot:
    def test_snapshot_completes(self):
        src, tx, snk, _ = make_linear_pipeline(n_msgs=8)
        for n in (src, tx, snk):
            n.start()
        time.sleep(0.3)   # let a few messages flow

        snapshots: List[GlobalSnapshot] = []
        coord = SnapshotCoordinator(
            "snap-lin", ["src", "tx", "snk"], on_complete=snapshots.append
        )
        for n in (src, tx, snk):
            n.set_coordinator(coord)
        src.initiate_snapshot("snap-lin")

        assert coord._fired is False or len(snapshots) > 0
        # Wait up to 5 s for snapshot to complete
        deadline = time.time() + 5.0
        while not snapshots and time.time() < deadline:
            time.sleep(0.05)

        for n in (src, tx, snk):
            n.stop()

        assert len(snapshots) == 1
        gs = snapshots[0]
        assert set(gs.node_snapshots.keys()) == {"src", "tx", "snk"}
        assert gs.complete is True

    def test_snapshot_state_is_non_decreasing(self):
        """Snapshot taken later should reflect at least as many processed msgs."""
        src, tx, snk, _ = make_linear_pipeline(n_msgs=10)
        for n in (src, tx, snk):
            n.start()

        def snap(snap_id):
            result: List[GlobalSnapshot] = []
            coord = SnapshotCoordinator(
                snap_id, ["src", "tx", "snk"], on_complete=result.append
            )
            for n in (src, tx, snk):
                n.set_coordinator(coord)
            src.initiate_snapshot(snap_id)
            deadline = time.time() + 5.0
            while not result and time.time() < deadline:
                time.sleep(0.02)
            return result[0]

        time.sleep(0.15)
        gs1 = snap("snap-t1")
        time.sleep(0.2)
        gs2 = snap("snap-t2")

        for n in (src, tx, snk):
            n.stop()

        seq1 = gs1.node_snapshots["src"].state["next_seq"]
        seq2 = gs2.node_snapshots["src"].state["next_seq"]
        assert seq2 >= seq1, "Later snapshot must reflect equal-or-greater progress"


# ── integration: full Pipeline.take_snapshot() ────────────────────────────────


class TestPipelineSnapshot:
    def test_full_snapshot_all_nodes_present(self):
        p = Pipeline(source_total=10, source_interval=0.05)
        p.start()
        time.sleep(0.5)
        gs = p.take_snapshot()
        p.stop()
        assert gs.complete
        expected = {"SourceA", "SourceB", "SlowTx", "Merge", "Aggregator", "Sink"}
        assert set(gs.node_snapshots.keys()) == expected

    def test_snapshot_aggregator_state_makes_sense(self):
        p = Pipeline(source_total=8, source_interval=0.04)
        p.start()
        time.sleep(0.6)
        gs = p.take_snapshot()
        p.stop()
        agg_state = gs.node_snapshots["Aggregator"].state
        assert agg_state["count"] >= 0
        assert agg_state["sum"] >= 0
        # sum should be consistent with count
        if agg_state["count"] > 0:
            assert agg_state["sum"] > 0
