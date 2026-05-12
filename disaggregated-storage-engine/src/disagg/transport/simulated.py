"""In-process RDMA simulator.

Implements `Transport.call` by directly invoking a handler on the server
object, after sleeping for `latency_us`. Adds optional jitter and a drop
probability for fault-injection tests.

Realistic numbers (single-machine):
    intra-rack RDMA:   2-5  µs
    cross-rack:         10  µs
    cross-AZ (cloud):  100  µs
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from disagg.transport.api import Transport


@dataclass
class SimulatedTransport(Transport):
    """In-process transport with configurable latency + jitter."""

    server: Any                              # something with a `dispatch(op, **kwargs)`
    latency_us: float = 5.0                  # base round-trip
    jitter_us: float = 0.5                   # ±jitter
    drop_rate: float = 0.0                   # probability call raises
    seed: int = 0
    _rng: random.Random = field(init=False)
    _stats_lock: threading.Lock = field(default_factory=threading.Lock)
    _stats: dict[str, int] = field(default_factory=lambda: {
        "n_calls": 0, "bytes_sent": 0, "bytes_received": 0, "dropped": 0,
    })

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)

    def _hop(self) -> None:
        if self.drop_rate > 0 and self._rng.random() < self.drop_rate:
            with self._stats_lock:
                self._stats["dropped"] += 1
            raise TransportError("simulated drop")
        delay_us = self.latency_us + self._rng.uniform(-self.jitter_us, self.jitter_us)
        if delay_us > 0:
            time.sleep(delay_us / 1e6)

    def call(self, op: str, **kwargs: Any) -> Any:
        self._hop()
        result = self.server.dispatch(op, **kwargs)
        with self._stats_lock:
            self._stats["n_calls"] += 1
            self._stats["bytes_sent"] += sum(
                len(v) if isinstance(v, bytes) else 8 for v in kwargs.values())
            self._stats["bytes_received"] += (
                len(result) if isinstance(result, bytes) else 8
            )
        return result

    def stats(self) -> dict[str, int]:
        with self._stats_lock:
            return dict(self._stats)


class TransportError(Exception):
    """Raised for simulated network failures."""
