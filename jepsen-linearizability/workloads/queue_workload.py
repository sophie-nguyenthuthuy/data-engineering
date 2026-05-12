"""Queue workload stub.

A proper FIFO queue workload would test enqueue/dequeue operations
for linearizability against QueueModel. This stub wires up the config;
a full cluster-side queue node is left as an extension point.
"""

from __future__ import annotations

from ..jepsen.core.runner import TestConfig
from ..jepsen.core.models import QueueModel


def queue_config(
    nodes: int = 3,
    clients: int = 4,
    duration: float = 10.0,
) -> TestConfig:
    return TestConfig(
        node_count=nodes,
        client_count=clients,
        test_duration_s=duration,
        keys=["q"],
        model="queue",
    )
