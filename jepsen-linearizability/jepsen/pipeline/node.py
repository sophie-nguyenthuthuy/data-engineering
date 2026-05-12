"""Pipeline node: a replicated key-value store running in its own process.

Each node:
  - Maintains a local KV store with per-key version counters.
  - Handles read/write requests from clients.
  - Broadcasts writes to peer nodes via the cluster router.
  - Uses "last write wins" by logical version (intentionally racy under
    clock skew to produce anomalies the checker can catch).

Communication protocol (all messages are plain dicts):
  Request  → {'type': 'req',       'req_id': str, 'op': 'read'|'write', 'key': str, 'value': Any}
  Response ← {'type': 'res',       'req_id': str, 'status': 'ok'|'fail', 'value': Any}
  Replicate→ {'type': 'replicate', 'key': str, 'value': Any, 'version': int, 'from': int}
"""

from __future__ import annotations

import multiprocessing as mp
import queue
import time
from typing import Any, Dict, Optional


def node_worker(
    node_id: int,
    inbox: mp.Queue,
    router_queue: mp.Queue,
    clock_offsets: mp.Array,  # shared float array indexed by node_id
    ready_event: mp.Event,
) -> None:
    """Main loop for a pipeline node process."""
    store: Dict[str, Any] = {}
    versions: Dict[str, int] = {}

    ready_event.set()

    while True:
        try:
            msg = inbox.get(timeout=0.1)
        except queue.Empty:
            continue

        if msg is None:  # shutdown signal
            break

        mtype = msg.get("type")

        if mtype == "req":
            req_id = msg["req_id"]
            op = msg["op"]
            key = msg.get("key")

            if op == "read":
                value = store.get(key, None)
                router_queue.put({
                    "type": "res",
                    "req_id": req_id,
                    "status": "ok",
                    "value": value,
                })

            elif op == "write":
                value = msg["value"]
                now = time.monotonic() + clock_offsets[node_id]
                new_version = int(now * 1_000_000)

                if new_version > versions.get(key, -1):
                    store[key] = value
                    versions[key] = new_version

                # Broadcast to peers
                router_queue.put({
                    "type": "broadcast",
                    "from": node_id,
                    "key": key,
                    "value": value,
                    "version": new_version,
                })

                router_queue.put({
                    "type": "res",
                    "req_id": req_id,
                    "status": "ok",
                    "value": "ok",
                })

            else:
                router_queue.put({
                    "type": "res",
                    "req_id": req_id,
                    "status": "fail",
                    "value": f"unknown op: {op}",
                })

        elif mtype == "replicate":
            key = msg["key"]
            value = msg["value"]
            version = msg["version"]
            if version > versions.get(key, -1):
                store[key] = value
                versions[key] = version
