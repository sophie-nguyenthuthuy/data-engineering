"""
Pipeline assembly, snapshot orchestration, and recovery.

Topology used in the demo:

    SourceA ──────────────────────────────────────────►╮
                                                       MergeNode → AggregatorNode → SinkNode
    SourceB ──► SlowTransformNode (×2 + 0.1 s delay) ►╯

Having two input channels on MergeNode is essential: when the fast path
(SourceA) delivers its marker first, MergeNode records its state and begins
recording the slow path channel.  Messages from SourceB that have already
left SlowTransformNode but haven't yet reached MergeNode are captured as
in-transit channel state — the canonical demonstration of Chandy-Lamport.

Recovery procedure:
  1. Stop all nodes.
  2. Drain all channels (discard any post-snapshot, pre-failure messages).
  3. Restore each node's state from its NodeSnapshot.
  4. Re-inject in-transit messages into the appropriate channels.
  5. Restart all nodes.
  6. Source nodes resume from their checkpointed offset + 1.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional

from .channel import Channel
from .message import DataMessage
from .node import (
    AggregatorNode,
    MergeNode,
    Node,
    SinkNode,
    SlowTransformNode,
    SourceNode,
)
from .snapshot import GlobalSnapshot, SnapshotCoordinator

log = logging.getLogger(__name__)


class Pipeline:
    """Assembles and manages the multi-node streaming pipeline."""

    def __init__(self, source_total: int = 20, source_interval: float = 0.06) -> None:
        # Nodes
        self.source_a = SourceNode("SourceA", total=source_total,
                                   interval=source_interval)
        self.source_b = SourceNode("SourceB", total=source_total,
                                   interval=source_interval)
        self.slow_tx = SlowTransformNode("SlowTx", fn=lambda x: x * 2, delay=0.12)
        self.merge = MergeNode("Merge")
        self.aggregator = AggregatorNode("Aggregator")
        self.sink = SinkNode("Sink")

        # Channels
        self.ch_a_merge = Channel("SourceA", "Merge")
        self.ch_b_slow = Channel("SourceB", "SlowTx")
        self.ch_slow_merge = Channel("SlowTx", "Merge")
        self.ch_merge_agg = Channel("Merge", "Aggregator")
        self.ch_agg_sink = Channel("Aggregator", "Sink")

        # Wire up
        self.source_a.add_out_channel(self.ch_a_merge)
        self.source_b.add_out_channel(self.ch_b_slow)

        self.slow_tx.add_in_channel(self.ch_b_slow)
        self.slow_tx.add_out_channel(self.ch_slow_merge)

        self.merge.add_in_channel(self.ch_a_merge)
        self.merge.add_in_channel(self.ch_slow_merge)
        self.merge.add_out_channel(self.ch_merge_agg)

        self.aggregator.add_in_channel(self.ch_merge_agg)
        self.aggregator.add_out_channel(self.ch_agg_sink)

        self.sink.add_in_channel(self.ch_agg_sink)

        self._all_nodes: List[Node] = [
            self.source_a, self.source_b, self.slow_tx,
            self.merge, self.aggregator, self.sink,
        ]
        self._source_nodes: List[Node] = [self.source_a, self.source_b]

        self._last_snapshot: Optional[GlobalSnapshot] = None
        self._snap_event = threading.Event()

        # Setup initial state for each node
        for n in self._all_nodes:
            n.setup()

    @property
    def all_nodes(self) -> List[Node]:
        return self._all_nodes

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        for n in self._all_nodes:
            n.start()
        log.info("Pipeline started")

    def stop(self) -> None:
        for n in self._all_nodes:
            n.stop()
        log.info("Pipeline stopped")

    def stop_node(self, node: Node) -> None:
        """Simulate a single node failure."""
        node.stop()
        log.warning(f"[Pipeline] Node {node.node_id} failed (simulated)")

    # ── snapshot ──────────────────────────────────────────────────────────────

    def take_snapshot(self) -> GlobalSnapshot:
        """
        Initiate a Chandy-Lamport snapshot and block until complete.

        Returns the assembled GlobalSnapshot.
        """
        snap_id = f"snap-{uuid.uuid4().hex[:8]}"
        log.info(f"[Pipeline] Initiating snapshot {snap_id[:12]}")

        self._snap_event.clear()
        self._last_snapshot = None

        coord = SnapshotCoordinator(
            snapshot_id=snap_id,
            node_ids=[n.node_id for n in self._all_nodes],
            on_complete=self._on_snapshot_complete,
        )
        for n in self._all_nodes:
            n.set_coordinator(coord)

        # Kick off from all source nodes simultaneously
        for src in self._source_nodes:
            src.initiate_snapshot(snap_id)

        if not self._snap_event.wait(timeout=15.0):
            raise TimeoutError("Snapshot did not complete within 15 s")

        assert self._last_snapshot is not None
        return self._last_snapshot

    def _on_snapshot_complete(self, gs: GlobalSnapshot) -> None:
        log.info(f"[Pipeline] Global snapshot complete: {gs}")
        self._last_snapshot = gs
        self._snap_event.set()

    # ── recovery ──────────────────────────────────────────────────────────────

    def recover(self, snapshot: GlobalSnapshot) -> None:
        """
        Restore the pipeline to the state captured in *snapshot*.

        Steps:
          1. Stop all running nodes.
          2. Drain all channels (discard post-snapshot messages).
          3. Restore each node's state.
          4. Re-inject in-transit messages.
          5. Restart all nodes.

        After this call the sources resume from their checkpointed offset,
        so no message will be emitted twice.  Combined with the idempotency
        guard in Node._handle_data, this gives exactly-once semantics.
        """
        log.info(f"[Pipeline] *** RECOVERY from {snapshot.snapshot_id[:12]} ***")

        # 1. Stop all nodes
        for n in self._all_nodes:
            n.stop()

        all_channels = [
            self.ch_a_merge, self.ch_b_slow, self.ch_slow_merge,
            self.ch_merge_agg, self.ch_agg_sink,
        ]

        # 2. Drain channels
        for ch in all_channels:
            discarded = ch.drain()
            if discarded:
                log.debug(f"[Recovery] Drained {len(discarded)} item(s) from {ch.name}")

        # 3. Restore node states
        for n in self._all_nodes:
            ns = snapshot.node_snapshots.get(n.node_id)
            if ns is None:
                log.warning(f"[Recovery] No snapshot for {n.node_id}, using init state")
                n.setup()
            else:
                n.restore_state(ns.state)

        # 4. Re-inject in-transit messages into the appropriate channels
        channel_map: Dict[str, Channel] = {ch.name: ch for ch in all_channels}
        total_replayed = 0
        for ns in snapshot.node_snapshots.values():
            for ch_name, msgs in ns.channel_states.items():
                if not msgs:
                    continue
                ch = channel_map.get(ch_name)
                if ch is None:
                    log.error(f"[Recovery] Unknown channel {ch_name}")
                    continue
                log.info(
                    f"[Recovery] Re-injecting {len(msgs)} in-transit msg(s) → {ch_name}"
                )
                for m in msgs:
                    ch.send(m)
                total_replayed += len(msgs)

        log.info(f"[Recovery] Replayed {total_replayed} in-transit message(s) total")

        # 5. Restart all nodes
        for n in self._all_nodes:
            n.start()

        log.info("[Pipeline] Recovery complete — pipeline resumed")
