"""Pipeline topology: directed acyclic graph of job nodes."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


@dataclass
class JobNode:
    job_id: str
    upstream_ids: list[str] = field(default_factory=list)
    downstream_ids: list[str] = field(default_factory=list)
    propagation_weight: float = 1.0  # multiplier when receiving propagated throttle


class PipelineTopology:
    """
    Represents a DAG of streaming jobs.

    Backpressure propagates *upstream*: when a job signals backpressure, the
    coordinator walks its transitive upstream ancestors and issues throttle commands
    scaled by hop distance and propagation weight.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, JobNode] = {}

    def add_job(self, job_id: str, propagation_weight: float = 1.0) -> JobNode:
        node = JobNode(job_id=job_id, propagation_weight=propagation_weight)
        self._nodes[job_id] = node
        return node

    def add_edge(self, upstream_id: str, downstream_id: str) -> None:
        """Register a data-flow edge: upstream → downstream."""
        if upstream_id not in self._nodes:
            raise KeyError(f"Unknown job: {upstream_id}")
        if downstream_id not in self._nodes:
            raise KeyError(f"Unknown job: {downstream_id}")
        self._nodes[upstream_id].downstream_ids.append(downstream_id)
        self._nodes[downstream_id].upstream_ids.append(upstream_id)

    def upstream_ancestors(self, job_id: str) -> dict[str, int]:
        """BFS from job_id going upstream; returns {job_id: hop_distance}."""
        visited: dict[str, int] = {}
        queue = [(job_id, 0)]
        while queue:
            current, depth = queue.pop(0)
            node = self._nodes.get(current)
            if node is None:
                continue
            for uid in node.upstream_ids:
                if uid not in visited:
                    visited[uid] = depth + 1
                    queue.append((uid, depth + 1))
        return visited

    def downstream_descendants(self, job_id: str) -> dict[str, int]:
        """BFS from job_id going downstream; returns {job_id: hop_distance}."""
        visited: dict[str, int] = {}
        queue = [(job_id, 0)]
        while queue:
            current, depth = queue.pop(0)
            node = self._nodes.get(current)
            if node is None:
                continue
            for did in node.downstream_ids:
                if did not in visited:
                    visited[did] = depth + 1
                    queue.append((did, depth + 1))
        return visited

    def all_jobs(self) -> Iterable[JobNode]:
        return self._nodes.values()

    def get_node(self, job_id: str) -> JobNode:
        return self._nodes[job_id]

    def __contains__(self, job_id: str) -> bool:
        return job_id in self._nodes

    @classmethod
    def linear(cls, *job_ids: str) -> PipelineTopology:
        """Convenience: build a simple linear chain A→B→C→…"""
        topo = cls()
        for jid in job_ids:
            topo.add_job(jid)
        for i in range(len(job_ids) - 1):
            topo.add_edge(job_ids[i], job_ids[i + 1])
        return topo
