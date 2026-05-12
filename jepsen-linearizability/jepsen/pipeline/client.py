"""Concurrent workload client.

Each Client runs in its own thread, issuing reads and writes to random
nodes. Every operation is bracketed with invoke/ok/fail records so the
history captures the exact real-time interval of each operation.
"""

from __future__ import annotations

import random
import threading
import time
from typing import Any, Callable, List, Optional

from ..core.history import History, Op
from .cluster import Cluster, NodeDeadError


class Client:
    def __init__(
        self,
        process_id: int,
        cluster: Cluster,
        history: History,
        keys: List[str],
    ) -> None:
        self.process_id = process_id
        self.cluster = cluster
        self.history = history
        self.keys = keys
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while self._running:
            op = random.choice(["read", "write"])
            key = random.choice(self.keys)
            node_id = random.choice(self.cluster.node_ids)

            if op == "write":
                value = random.randint(0, 999)
                self._do_write(node_id, key, value)
            else:
                self._do_read(node_id, key)

            time.sleep(random.uniform(0.005, 0.03))

    def _do_write(self, node_id: int, key: str, value: int) -> None:
        self.history.record(Op(self.process_id, "invoke", "write", (key, value)))
        try:
            result = self.cluster.write(node_id, key, value)
            self.history.record(Op(self.process_id, "ok", "write", result))
        except NodeDeadError:
            self.history.record(Op(self.process_id, "fail", "write", "node-dead"))
        except TimeoutError:
            self.history.record(Op(self.process_id, "info", "write", "timeout"))
        except Exception as e:
            self.history.record(Op(self.process_id, "fail", "write", str(e)))

    def _do_read(self, node_id: int, key: str) -> None:
        self.history.record(Op(self.process_id, "invoke", "read", key))
        try:
            value = self.cluster.read(node_id, key)
            self.history.record(Op(self.process_id, "ok", "read", value))
        except NodeDeadError:
            self.history.record(Op(self.process_id, "fail", "read", "node-dead"))
        except TimeoutError:
            self.history.record(Op(self.process_id, "info", "read", "timeout"))
        except Exception as e:
            self.history.record(Op(self.process_id, "fail", "read", str(e)))
