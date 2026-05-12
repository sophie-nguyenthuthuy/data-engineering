"""Cluster: manages node processes and routes messages with chaos support.

The cluster acts as the network fabric between nodes. It checks the
partition table before delivering any inter-node message, providing
application-layer network partitions without OS-level tc/iptables.

Architecture:
  - N node worker processes, each with an inbox Queue
  - A router thread in the main process that dispatches messages
  - A response map that lets clients await specific request IDs
  - Shared clock_offsets array for ClockRegistry integration
"""

from __future__ import annotations

import ctypes
import multiprocessing as mp
import queue
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from .node import node_worker
from ..chaos.network import PartitionTable
from ..chaos.clock import ClockRegistry
from ..chaos.process import ProcessRegistry


class Cluster:
    def __init__(
        self,
        node_count: int = 3,
        request_timeout: float = 1.0,
    ) -> None:
        self.node_count = node_count
        self.node_ids: List[int] = list(range(node_count))
        self.request_timeout = request_timeout

        self.partition_table = PartitionTable()
        self.clock_registry = ClockRegistry()
        self.process_registry = ProcessRegistry()

        # Shared float array for clock offsets (readable from node subprocesses)
        self._clock_offsets = mp.Array(ctypes.c_double, node_count)

        self._router_queue: mp.Queue = mp.Queue()
        self._inboxes: Dict[int, mp.Queue] = {}
        self._processes: Dict[int, mp.Process] = {}

        # Pending client responses: req_id -> threading.Event + result holder
        self._pending: Dict[str, dict] = {}
        self._pending_lock = threading.Lock()

        self._router_thread: Optional[threading.Thread] = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._running = True
        for node_id in self.node_ids:
            self._start_node(node_id)

        self._router_thread = threading.Thread(target=self._route_loop, daemon=True)
        self._router_thread.start()

    def _start_node(self, node_id: int) -> None:
        inbox: mp.Queue = mp.Queue()
        ready = mp.Event()
        p = mp.Process(
            target=node_worker,
            args=(node_id, inbox, self._router_queue, self._clock_offsets, ready),
            daemon=True,
        )
        p.start()
        ready.wait(timeout=5)
        self._inboxes[node_id] = inbox
        self._processes[node_id] = p
        self.process_registry.register(node_id, p.pid)

    def stop(self) -> None:
        self._running = False
        for inbox in self._inboxes.values():
            try:
                inbox.put(None)
            except Exception:
                pass
        for p in self._processes.values():
            p.join(timeout=2)
            if p.is_alive():
                p.kill()

    # ------------------------------------------------------------------
    # Client request API
    # ------------------------------------------------------------------

    def read(self, node_id: int, key: str) -> Any:
        return self._request(node_id, {"op": "read", "key": key})

    def write(self, node_id: int, key: str, value: Any) -> str:
        return self._request(node_id, {"op": "write", "key": key, "value": value})

    def _request(self, node_id: int, payload: dict) -> Any:
        if not self.process_registry.is_alive(node_id):
            raise NodeDeadError(node_id)

        req_id = str(uuid.uuid4())
        event = threading.Event()
        holder: dict = {}

        with self._pending_lock:
            self._pending[req_id] = {"event": event, "result": holder}

        msg = {"type": "req", "req_id": req_id, **payload}
        latency = self.partition_table.extra_latency(node_id, node_id)  # self-latency unused
        self._inboxes[node_id].put(msg)

        if not event.wait(timeout=self.request_timeout):
            with self._pending_lock:
                self._pending.pop(req_id, None)
            raise TimeoutError(f"node {node_id} did not respond to {payload['op']} in {self.request_timeout}s")

        with self._pending_lock:
            self._pending.pop(req_id, None)

        if holder.get("status") == "fail":
            raise RequestFailedError(holder.get("value"))

        return holder.get("value")

    # ------------------------------------------------------------------
    # Router loop (main process, daemon thread)
    # ------------------------------------------------------------------

    def _route_loop(self) -> None:
        while self._running:
            try:
                msg = self._router_queue.get(timeout=0.05)
            except queue.Empty:
                continue

            mtype = msg.get("type")

            if mtype == "res":
                self._deliver_response(msg)

            elif mtype == "broadcast":
                self._broadcast_replication(msg)

    def _deliver_response(self, msg: dict) -> None:
        req_id = msg["req_id"]
        with self._pending_lock:
            entry = self._pending.get(req_id)
        if entry is None:
            return
        entry["result"]["status"] = msg["status"]
        entry["result"]["value"] = msg["value"]
        entry["event"].set()

    def _broadcast_replication(self, msg: dict) -> None:
        from_node = msg["from"]
        repl_msg = {
            "type": "replicate",
            "key": msg["key"],
            "value": msg["value"],
            "version": msg["version"],
            "from": from_node,
        }
        for node_id in self.node_ids:
            if node_id == from_node:
                continue
            if self.partition_table.is_partitioned(from_node, node_id):
                continue  # message dropped
            if not self.process_registry.is_alive(node_id):
                continue
            latency = self.partition_table.extra_latency(from_node, node_id)
            if latency > 0:
                threading.Thread(
                    target=self._delayed_deliver,
                    args=(node_id, repl_msg, latency / 1000.0),
                    daemon=True,
                ).start()
            else:
                self._inboxes[node_id].put(repl_msg)

    def _delayed_deliver(self, node_id: int, msg: dict, delay: float) -> None:
        time.sleep(delay)
        if self.process_registry.is_alive(node_id):
            self._inboxes[node_id].put(msg)

    # ------------------------------------------------------------------
    # Clock skew integration
    # ------------------------------------------------------------------

    def apply_clock_offsets(self) -> None:
        """Sync ClockRegistry offsets into the shared array for node processes."""
        offsets = self.clock_registry.offsets()
        for node_id in self.node_ids:
            self._clock_offsets[node_id] = offsets.get(node_id, 0.0)

    # ------------------------------------------------------------------
    # Process crash/restart integration
    # ------------------------------------------------------------------

    def restart_node(self, node_id: int) -> None:
        """Kill and restart a node process (used by ProcessCrashNemesis)."""
        old = self._processes.get(node_id)
        if old and old.is_alive():
            old.kill()
            old.join(timeout=1)
        self._start_node(node_id)

    def kill_node(self, node_id: int) -> None:
        p = self._processes.get(node_id)
        if p and p.is_alive():
            p.kill()
            p.join(timeout=1)
        self.process_registry.mark_dead(node_id)


class NodeDeadError(Exception):
    def __init__(self, node_id: int) -> None:
        super().__init__(f"node {node_id} is dead")
        self.node_id = node_id


class RequestFailedError(Exception):
    pass
