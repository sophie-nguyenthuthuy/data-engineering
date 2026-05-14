"""Fair scheduler.

Deficit Round-Robin (DRR, Shreedhar & Varghese 1995) across tenants:
each tenant has a "deficit counter" incremented by its weight at every
quantum; jobs are served while the counter has enough budget for the
job's cost. This gives a clean weighted fair-share without packet-loss
or starvation.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mtip.registry.tenant import Tenant


@dataclass(frozen=True, slots=True)
class Job:
    """Unit of work submitted by a tenant."""

    tenant_id: str
    job_id: str
    cost: float = 1.0  # CPU-time-equivalent

    def __post_init__(self) -> None:
        if not self.tenant_id:
            raise ValueError("tenant_id must be non-empty")
        if not self.job_id:
            raise ValueError("job_id must be non-empty")
        if self.cost <= 0:
            raise ValueError("cost must be > 0")


@dataclass(frozen=True, slots=True)
class Scheduled:
    """Job + its position in the dispatch order."""

    order: int
    job: Job


@dataclass
class FairScheduler:
    """Weighted Deficit Round Robin scheduler.

    Weights default to each tenant's CPU quota; pass an explicit
    ``weights`` map to override.
    """

    queues: dict[str, deque[Job]] = field(default_factory=dict)
    weights: dict[str, float] = field(default_factory=dict)
    quantum: float = 1.0
    _deficits: dict[str, float] = field(default_factory=dict, repr=False)
    _order: list[str] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        if self.quantum <= 0:
            raise ValueError("quantum must be > 0")

    # ---------------------------------------------------------------- API

    def add_tenant(self, tenant: Tenant, weight: float | None = None) -> None:
        if tenant.id in self.queues:
            return
        self.queues[tenant.id] = deque()
        self.weights[tenant.id] = weight if weight is not None else tenant.quota.cpu_cores
        self._deficits[tenant.id] = 0.0
        self._order.append(tenant.id)

    def submit(self, job: Job) -> None:
        if job.tenant_id not in self.queues:
            raise KeyError(f"unknown tenant {job.tenant_id!r}")
        self.queues[job.tenant_id].append(job)

    def schedule(self, n: int) -> list[Scheduled]:
        """Drain up to ``n`` jobs in fair order; raises on n<1."""
        if n < 1:
            raise ValueError("n must be ≥ 1")
        out: list[Scheduled] = []
        rotations = 0
        max_rotations = n * 4  # safety
        while len(out) < n:
            if not any(self.queues.values()):
                break
            rotations += 1
            if rotations > max_rotations:
                break
            for tid in self._order:
                q = self.queues.get(tid)
                if not q:
                    continue
                self._deficits[tid] += self.weights[tid] * self.quantum
                while q and self._deficits[tid] >= q[0].cost:
                    job = q.popleft()
                    self._deficits[tid] -= job.cost
                    out.append(Scheduled(order=len(out), job=job))
                    if len(out) >= n:
                        return out
        return out


__all__ = ["FairScheduler", "Job", "Scheduled"]
