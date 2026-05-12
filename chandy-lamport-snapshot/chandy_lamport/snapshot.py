"""Snapshot data structures and coordinator."""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .message import DataMessage


@dataclass
class NodeSnapshot:
    """
    A single node's contribution to a global consistent snapshot.

    channel_states maps each *incoming* channel name to the list of
    DataMessages that were in-transit on that channel at snapshot time.
    For the node that first receives a marker on channel C, that channel's
    state is empty (all pre-snapshot messages arrived before the marker,
    thanks to FIFO ordering).  For nodes with multiple inputs, channels
    whose markers arrive *after* the node recorded its state may carry
    in-transit messages.
    """

    node_id: str
    state: Any
    channel_states: Dict[str, List[DataMessage]] = field(default_factory=dict)

    def __repr__(self) -> str:
        ch = {k: len(v) for k, v in self.channel_states.items()}
        return f"NodeSnapshot({self.node_id}, state={self.state!r}, in_transit={ch})"


@dataclass
class GlobalSnapshot:
    """
    A globally consistent snapshot assembled from all NodeSnapshots.

    Consistency guarantee: for every message M that was received by node Q
    before Q's local snapshot, M was also sent by node P before P's local
    snapshot.  This follows from FIFO channels and the marker propagation
    protocol.
    """

    snapshot_id: str
    node_snapshots: Dict[str, NodeSnapshot] = field(default_factory=dict)
    complete: bool = False

    def add(self, ns: NodeSnapshot) -> None:
        self.node_snapshots[ns.node_id] = ns

    def describe(self) -> str:
        lines = [f"\n{'='*60}", f"  Global Snapshot  [{self.snapshot_id[:8]}]", f"{'='*60}"]
        for ns in self.node_snapshots.values():
            lines.append(f"  {ns}")
        lines.append(f"{'='*60}\n")
        return "\n".join(lines)

    def __repr__(self) -> str:
        return (f"GlobalSnapshot(id={self.snapshot_id[:8]}, "
                f"nodes={list(self.node_snapshots.keys())}, complete={self.complete})")


class SnapshotCoordinator:
    """
    Collects NodeSnapshots from every node and assembles a GlobalSnapshot
    once all nodes have reported in.
    """

    def __init__(
        self,
        snapshot_id: str,
        node_ids: List[str],
        on_complete: Callable[[GlobalSnapshot], None],
    ) -> None:
        self.snapshot_id = snapshot_id
        self._expected = set(node_ids)
        self._received: Dict[str, NodeSnapshot] = {}
        self._lock = threading.Lock()
        self._on_complete = on_complete
        self._fired = False

    def receive(self, ns: NodeSnapshot) -> None:
        completed: Optional[GlobalSnapshot] = None
        with self._lock:
            if self._fired:
                return
            self._received[ns.node_id] = ns
            if set(self._received.keys()) >= self._expected:
                self._fired = True
                gs = GlobalSnapshot(
                    snapshot_id=self.snapshot_id,
                    node_snapshots=dict(self._received),
                    complete=True,
                )
                completed = gs

        if completed is not None:
            self._on_complete(completed)
