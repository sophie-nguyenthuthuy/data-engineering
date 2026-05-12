"""Register workload: concurrent reads and writes to a set of keys.

This workload is the simplest interesting case for linearizability:
a shared register (or set of registers) accessed by multiple concurrent
clients. Under network partitions or clock skew in a multi-node cluster
using last-write-wins replication, reads may return stale values,
violating linearizability.
"""

from __future__ import annotations

from ..jepsen.core.runner import TestConfig, run, TestResult


def register_workload(
    nodes: int = 3,
    clients: int = 5,
    duration: float = 10.0,
    keys: list[str] | None = None,
    **chaos_flags,
) -> TestResult:
    config = TestConfig(
        node_count=nodes,
        client_count=clients,
        test_duration_s=duration,
        keys=keys or ["x", "y", "z"],
        model="register",
        **chaos_flags,
    )
    return run(config)
