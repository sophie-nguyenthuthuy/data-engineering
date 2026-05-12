"""Process crash and restart injection.

Sends SIGKILL to a random node process, then optionally restarts it
after a configurable delay. The cluster tracks which nodes are alive
and returns errors for requests to dead nodes.
"""

from __future__ import annotations

import os
import random
import signal
import threading
import time
from typing import Callable, Dict, List, Optional

from .nemesis import Nemesis


class ProcessRegistry:
    """Tracks live/dead state for node processes."""

    def __init__(self) -> None:
        self._pids: Dict[int, int] = {}       # node_id -> pid
        self._dead: set[int] = set()
        self._lock = threading.Lock()

    def register(self, node_id: int, pid: int) -> None:
        with self._lock:
            self._pids[node_id] = pid
            self._dead.discard(node_id)

    def is_alive(self, node_id: int) -> bool:
        with self._lock:
            return node_id not in self._dead

    def kill(self, node_id: int) -> bool:
        with self._lock:
            pid = self._pids.get(node_id)
            if pid is None:
                return False
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self._dead.add(node_id)
            return True

    def mark_dead(self, node_id: int) -> None:
        with self._lock:
            self._dead.add(node_id)

    def mark_alive(self, node_id: int) -> None:
        with self._lock:
            self._dead.discard(node_id)

    def dead_nodes(self) -> List[int]:
        with self._lock:
            return list(self._dead)

    def all_nodes(self) -> List[int]:
        with self._lock:
            return list(self._pids.keys())


class ProcessCrashNemesis(Nemesis):
    """Kills a random node and optionally restarts it."""

    def __init__(
        self,
        registry: ProcessRegistry,
        restart_fn: Optional[Callable[[int], None]] = None,
        restart_delay: float = 2.0,
    ) -> None:
        self._registry = registry
        self._restart_fn = restart_fn
        self._restart_delay = restart_delay
        self._killed: list[int] = []

    def start(self) -> None:
        alive = [n for n in self._registry.all_nodes() if self._registry.is_alive(n)]
        if not alive:
            return
        victim = random.choice(alive)
        self._registry.kill(victim)
        self._killed = [victim]

    def stop(self) -> None:
        if self._restart_fn is None:
            for node_id in self._killed:
                self._registry.mark_alive(node_id)
        else:
            for node_id in self._killed:
                t = threading.Thread(
                    target=self._delayed_restart,
                    args=(node_id,),
                    daemon=True,
                )
                t.start()
        self._killed = []

    def _delayed_restart(self, node_id: int) -> None:
        time.sleep(self._restart_delay)
        self._restart_fn(node_id)  # type: ignore[misc]
        self._registry.mark_alive(node_id)

    def describe(self) -> str:
        return f"ProcessCrash(restart_delay={self._restart_delay}s)"
