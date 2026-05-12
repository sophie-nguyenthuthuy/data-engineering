"""Network partition and latency injection.

Partitions are implemented at the application message-routing layer:
the cluster checks the partition table before delivering each message,
so no OS-level iptables or tc manipulation is needed.
"""

from __future__ import annotations

import random
import threading
from typing import FrozenSet, Optional, Set, Tuple

from .nemesis import Nemesis


class PartitionTable:
    """Thread-safe partition table shared between the cluster and nemesis."""

    def __init__(self) -> None:
        self._partitions: Set[FrozenSet[int]] = set()
        self._lock = threading.RLock()
        self._latency_ms: dict[FrozenSet[int], float] = {}  # extra latency per pair

    def partition(self, a: int, b: int) -> None:
        with self._lock:
            self._partitions.add(frozenset({a, b}))

    def heal(self, a: int | None = None, b: int | None = None) -> None:
        with self._lock:
            if a is None:
                self._partitions.clear()
                self._latency_ms.clear()
            else:
                self._partitions.discard(frozenset({a, b}))
                self._latency_ms.pop(frozenset({a, b}), None)

    def is_partitioned(self, a: int, b: int) -> bool:
        with self._lock:
            return frozenset({a, b}) in self._partitions

    def extra_latency(self, a: int, b: int) -> float:
        with self._lock:
            return self._latency_ms.get(frozenset({a, b}), 0.0)

    def set_latency(self, a: int, b: int, ms: float) -> None:
        with self._lock:
            self._latency_ms[frozenset({a, b})] = ms

    def active_partitions(self) -> list[tuple[int, int]]:
        with self._lock:
            return [(min(p), max(p)) for p in self._partitions]


class NetworkPartitionNemesis(Nemesis):
    """Randomly partitions a subset of nodes from the rest."""

    def __init__(self, table: PartitionTable, node_ids: list[int]) -> None:
        self._table = table
        self._node_ids = node_ids
        self._active: list[tuple[int, int]] = []

    def start(self) -> None:
        if len(self._node_ids) < 2:
            return
        # Isolate one random node from all others
        victim = random.choice(self._node_ids)
        self._active = []
        for node in self._node_ids:
            if node != victim:
                self._table.partition(victim, node)
                self._active.append((victim, node))

    def stop(self) -> None:
        for a, b in self._active:
            self._table.heal(a, b)
        self._active = []

    def describe(self) -> str:
        return "NetworkPartition(random node isolation)"


class NetworkLatencyNemesis(Nemesis):
    """Injects artificial latency between random node pairs."""

    def __init__(
        self,
        table: PartitionTable,
        node_ids: list[int],
        latency_ms: float = 100.0,
    ) -> None:
        self._table = table
        self._node_ids = node_ids
        self._latency_ms = latency_ms
        self._active: list[tuple[int, int]] = []

    def start(self) -> None:
        if len(self._node_ids) < 2:
            return
        a, b = random.sample(self._node_ids, 2)
        self._table.set_latency(a, b, self._latency_ms)
        self._active = [(a, b)]

    def stop(self) -> None:
        for a, b in self._active:
            self._table.heal(a, b)
        self._active = []

    def describe(self) -> str:
        return f"NetworkLatency({self._latency_ms}ms)"
