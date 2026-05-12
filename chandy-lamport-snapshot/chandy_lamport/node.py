"""
Node implementations for the distributed pipeline.

Every node runs its processing loop in a dedicated daemon thread.
Snapshot participation follows the Chandy-Lamport algorithm:

  Phase 1 – Initiation (source nodes only, triggered by the coordinator):
    Record local state → send Marker on every outgoing channel.
    Because source nodes have no incoming channels, their local snapshot
    is immediately complete.

  Phase 2 – Propagation (all other nodes):
    On receiving the *first* Marker for snapshot S on channel C:
      a. Record local state.
      b. Mark channel C's state as empty (all pre-snapshot messages on C
         arrived before the marker, guaranteed by FIFO).
      c. Start recording every *other* incoming channel.
      d. Forward Marker on every outgoing channel.

    On receiving a *subsequent* Marker for S on channel C:
      a. Stop recording C → those recorded messages are C's in-transit state.

    Once all incoming channels have delivered their Marker, the node's
    local snapshot is complete and is submitted to the SnapshotCoordinator.

Recovery:
    Restore each node's state from its NodeSnapshot.
    Drain and discard all channels, then re-inject each channel's in-transit
    messages in order.  Source nodes resume emission from their checkpointed
    sequence number + 1.
"""
from __future__ import annotations

import copy
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from .channel import Channel
from .message import DataMessage, Marker
from .snapshot import NodeSnapshot, SnapshotCoordinator

log = logging.getLogger(__name__)


# ── base node ─────────────────────────────────────────────────────────────────


class Node:
    """
    Base class for all pipeline nodes.

    Subclasses override:
      init_state()  → initial state value
      process(content, state) → (output | None, new_state)
    """

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id

        self._state: Any = None
        self._state_lock = threading.Lock()

        self.in_channels: Dict[str, Channel] = {}   # channel.name → Channel
        self.out_channels: List[Channel] = []

        # Per-snapshot bookkeeping
        # snap_id → {state, channel_states, channels_done}
        self._snaps: Dict[str, dict] = {}
        self._snap_lock = threading.RLock()
        self._coordinator: Optional[SnapshotCoordinator] = None

        # Thread lifecycle
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Exactly-once deduplication: set of msg_ids already processed
        self._seen: set = set()

    # ── subclass interface ────────────────────────────────────────────────────

    def init_state(self) -> Any:
        return {}

    def process(self, content: Any, state: Any) -> Tuple[Any, Any]:
        """Return (output, new_state). Set output=None to drop the message."""
        return content, state

    # ── wiring ────────────────────────────────────────────────────────────────

    def setup(self) -> None:
        self._state = copy.deepcopy(self.init_state())

    def add_in_channel(self, ch: Channel) -> None:
        self.in_channels[ch.name] = ch

    def add_out_channel(self, ch: Channel) -> None:
        self.out_channels.append(ch)

    def set_coordinator(self, coord: SnapshotCoordinator) -> None:
        self._coordinator = coord

    # ── state ─────────────────────────────────────────────────────────────────

    def get_state(self) -> Any:
        with self._state_lock:
            return copy.deepcopy(self._state)

    def restore_state(self, state: Any) -> None:
        with self._state_lock:
            self._state = copy.deepcopy(state)
        self._seen.clear()
        log.info(f"[{self.node_id}] State restored → {state!r}")

    # ── Chandy-Lamport snapshot ────────────────────────────────────────────────

    def initiate_snapshot(self, snapshot_id: str) -> None:
        """
        Called by the coordinator on source nodes to kick off a new snapshot.
        Records local state, sends markers downstream, and immediately
        completes the local snapshot (no incoming channels to wait for).
        """
        log.info(f"[{self.node_id}] Initiating snapshot {snapshot_id[:8]}")
        with self._snap_lock:
            captured = self.get_state()
            self._snaps[snapshot_id] = {
                "state": captured,
                "channel_states": {},
                "channels_done": set(),
            }
            for ch in self.in_channels.values():
                ch.start_recording()

        m = Marker(snapshot_id=snapshot_id, initiator_id=self.node_id)
        for ch in self.out_channels:
            log.debug(f"[{self.node_id}] Sending marker → {ch.name}")
            ch.send(m)

        self._maybe_complete(snapshot_id)

    def _on_marker(self, marker: Marker, ch: Channel) -> None:
        snap_id = marker.snapshot_id
        log.debug(f"[{self.node_id}] ← {marker} on {ch.name}")

        with self._snap_lock:
            if snap_id not in self._snaps:
                # ── First marker for this snapshot ─────────────────────────
                # Record state immediately; this channel's state is empty
                # (FIFO guarantees all pre-snapshot messages arrived before
                # the marker).
                captured = self.get_state()
                self._snaps[snap_id] = {
                    "state": captured,
                    "channel_states": {ch.name: []},
                    "channels_done": {ch.name},
                }
                # Start recording every OTHER incoming channel
                for name, other in self.in_channels.items():
                    if name != ch.name:
                        other.start_recording()
                # Propagate marker downstream
                m = Marker(snapshot_id=snap_id, initiator_id=marker.initiator_id)
                for out in self.out_channels:
                    log.debug(f"[{self.node_id}] Propagating marker → {out.name}")
                    out.send(m)
            else:
                # ── Subsequent marker for this snapshot ────────────────────
                # Everything recorded on this channel since we took our local
                # state snapshot is in-transit and part of the global state.
                recorded = ch.stop_recording()
                snap = self._snaps[snap_id]
                snap["channel_states"][ch.name] = recorded
                snap["channels_done"].add(ch.name)
                log.debug(
                    f"[{self.node_id}] Channel {ch.name} in-transit: "
                    f"{len(recorded)} message(s)"
                )

        self._maybe_complete(snap_id)

    def _maybe_complete(self, snap_id: str) -> None:
        """Submit local snapshot if markers have arrived on all incoming channels."""
        with self._snap_lock:
            if snap_id not in self._snaps:
                return
            snap = self._snaps[snap_id]
            all_done = snap["channels_done"] >= set(self.in_channels.keys())

        if not all_done:
            return

        with self._snap_lock:
            snap = self._snaps.pop(snap_id, None)
        if snap is None:
            return

        ns = NodeSnapshot(
            node_id=self.node_id,
            state=snap["state"],
            channel_states=snap["channel_states"],
        )
        log.info(f"[{self.node_id}] Local snapshot complete: {ns}")
        if self._coordinator:
            self._coordinator.receive(ns)

    # ── message processing ────────────────────────────────────────────────────

    def _handle_data(self, msg: DataMessage) -> None:
        # Idempotency guard: skip messages we've already processed.
        # This protects exactly-once semantics during recovery replay.
        if msg.msg_id in self._seen:
            log.warning(f"[{self.node_id}] Skipping duplicate {msg}")
            return
        self._seen.add(msg.msg_id)

        with self._state_lock:
            output, self._state = self.process(msg.content, self._state)

        if output is not None:
            out_msg = DataMessage(
                content=output,
                msg_id=msg.msg_id,        # preserve ID for end-to-end tracing
                origin_seq=msg.origin_seq,
                sender_id=self.node_id,
            )
            for ch in self.out_channels:
                ch.send(out_msg)

    # ── run loop ──────────────────────────────────────────────────────────────

    def _run(self) -> None:
        while not self._stop.is_set():
            busy = False
            for ch in list(self.in_channels.values()):
                msg = ch.receive(timeout=0.01)
                if msg is None:
                    continue
                busy = True
                if isinstance(msg, Marker):
                    self._on_marker(msg, ch)
                else:
                    # Record BEFORE processing so in-transit capture is accurate
                    ch.record_if_needed(msg)
                    self._handle_data(msg)
            if not busy:
                time.sleep(0.001)

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name=f"node-{self.node_id}", daemon=True
        )
        self._thread.start()
        log.debug(f"[{self.node_id}] Started")

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        log.debug(f"[{self.node_id}] Stopped")


# ── concrete node types ───────────────────────────────────────────────────────


class SourceNode(Node):
    """
    Generates a stream of integers [1 .. total] on its outgoing channels.

    State: {"next_seq": int}  — the next sequence number to emit.
    On recovery, emission resumes from next_seq (the checkpoint offset + 1),
    ensuring no message is emitted twice.
    """

    def __init__(
        self,
        node_id: str,
        total: int,
        interval: float = 0.05,
        label: str = "",
    ) -> None:
        super().__init__(node_id)
        self._total = total
        self._interval = interval
        self._label = label or node_id

    def init_state(self) -> Any:
        return {"next_seq": 1}

    def _run(self) -> None:  # type: ignore[override]
        while not self._stop.is_set():
            with self._state_lock:
                seq = self._state["next_seq"]
                if seq > self._total:
                    # Finished; idle until stopped
                    time.sleep(0.01)
                    continue
                self._state["next_seq"] = seq + 1

            msg = DataMessage(content=seq, origin_seq=seq, sender_id=self.node_id)
            log.debug(f"[{self.node_id}] Emitting {msg}")
            for ch in self.out_channels:
                ch.send(msg)

            time.sleep(self._interval)


class TransformNode(Node):
    """Applies a pure function to each message's content."""

    def __init__(self, node_id: str, fn) -> None:
        super().__init__(node_id)
        self._fn = fn

    def init_state(self) -> Any:
        return {"processed": 0}

    def process(self, content: Any, state: Any) -> Tuple[Any, Any]:
        state["processed"] += 1
        return self._fn(content), state


class SlowTransformNode(TransformNode):
    """TransformNode with artificial per-message latency (demo purposes)."""

    def __init__(self, node_id: str, fn, delay: float = 0.1) -> None:
        super().__init__(node_id, fn)
        self._delay = delay

    def process(self, content: Any, state: Any) -> Tuple[Any, Any]:
        time.sleep(self._delay)
        return super().process(content, state)


class MergeNode(Node):
    """
    Merges messages from multiple input channels.

    State: {"counts": {channel_name: int}}
    This node is the key demonstration point for in-transit channel states:
    with multiple inputs it will almost certainly have messages in-flight
    on the slower channel when the first marker arrives.
    """

    def init_state(self) -> Any:
        return {"received": 0}

    def process(self, content: Any, state: Any) -> Tuple[Any, Any]:
        state["received"] += 1
        return content, state


class AggregatorNode(Node):
    """
    Maintains a running sum of all received integer values.

    State: {"sum": int, "count": int}
    This is the stateful core of the demo — its state must survive a failure
    and be restored exactly to the snapshot checkpoint.
    """

    def init_state(self) -> Any:
        return {"sum": 0, "count": 0}

    def process(self, content: Any, state: Any) -> Tuple[Any, Any]:
        state["sum"] += content
        state["count"] += 1
        # Forward both the value and the running sum for rich output
        return {"value": content, "running_sum": state["sum"]}, state


class SinkNode(Node):
    """
    Terminal node — records every received message for verification.

    State: {"received_seqs": sorted list of origin_seq values}
    """

    def __init__(self, node_id: str) -> None:
        super().__init__(node_id)
        self._results: List[Any] = []           # thread-safe via state_lock
        self._origin_seqs: List[int] = []

    def init_state(self) -> Any:
        return {"received_seqs": [], "count": 0}

    def process(self, content: Any, state: Any) -> Tuple[Any, Any]:
        state["received_seqs"].append(content.get("origin_seq", -1)
                                      if isinstance(content, dict) else -1)
        state["count"] += 1
        return None, state   # terminal; nothing to forward

    def _handle_data(self, msg: DataMessage) -> None:
        if msg.msg_id in self._seen:
            log.warning(f"[{self.node_id}] Skipping duplicate {msg}")
            return
        self._seen.add(msg.msg_id)

        with self._state_lock:
            self._state["count"] += 1
            self._state["received_seqs"].append(msg.origin_seq)
            self._results.append(msg.content)
            self._origin_seqs.append(msg.origin_seq)

    @property
    def results(self) -> List[Any]:
        with self._state_lock:
            return list(self._results)

    @property
    def received_seqs(self) -> List[int]:
        with self._state_lock:
            return sorted(self._origin_seqs)

    def restore_state(self, state: Any) -> None:
        super().restore_state(state)
        with self._state_lock:
            seqs = state.get("received_seqs", [])
            self._origin_seqs = list(seqs)
            # Rebuild results list conservatively (we don't store full content
            # in state, only sequence numbers)
            self._results = [f"(restored seq {s})" for s in seqs]
