"""Transport interface.

In production this is RDMA over `ucx-py` or InfiniBand verbs. We test
through a simulated implementation; alternate implementations must satisfy
the same blocking-call contract.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Transport(ABC):
    """A simple request-response transport. Each call is synchronous from
    the caller's POV; the implementation may inject latency."""

    @abstractmethod
    def call(self, op: str, **kwargs: Any) -> Any:
        """Issue a remote call. Blocks until the response is received."""

    @abstractmethod
    def stats(self) -> dict[str, int]:
        """Counters: bytes_sent, bytes_received, n_calls."""
