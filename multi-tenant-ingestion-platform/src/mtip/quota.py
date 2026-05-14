"""Resource quotas + live usage tracking.

A :class:`ResourceQuota` is the *limit*; a :class:`ResourceUsage`
tracks the live counter. The admission controller compares one against
the other on every job submission.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ResourceQuota:
    """Per-tenant resource caps."""

    cpu_cores: float
    storage_gb: float
    ingestion_qps: float

    def __post_init__(self) -> None:
        if self.cpu_cores <= 0:
            raise ValueError("cpu_cores must be > 0")
        if self.storage_gb <= 0:
            raise ValueError("storage_gb must be > 0")
        if self.ingestion_qps <= 0:
            raise ValueError("ingestion_qps must be > 0")


@dataclass
class ResourceUsage:
    """Mutable running totals; RLock-guarded."""

    cpu_cores_in_use: float = 0.0
    storage_gb_in_use: float = 0.0
    ingestion_qps_in_use: float = 0.0
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    # ----------------------------------------------------------- mutate

    def reserve(self, *, cpu: float = 0.0, storage: float = 0.0, qps: float = 0.0) -> None:
        if cpu < 0 or storage < 0 or qps < 0:
            raise ValueError("reservations must be non-negative")
        with self._lock:
            self.cpu_cores_in_use += cpu
            self.storage_gb_in_use += storage
            self.ingestion_qps_in_use += qps

    def release(self, *, cpu: float = 0.0, storage: float = 0.0, qps: float = 0.0) -> None:
        if cpu < 0 or storage < 0 or qps < 0:
            raise ValueError("releases must be non-negative")
        with self._lock:
            self.cpu_cores_in_use = max(0.0, self.cpu_cores_in_use - cpu)
            self.storage_gb_in_use = max(0.0, self.storage_gb_in_use - storage)
            self.ingestion_qps_in_use = max(0.0, self.ingestion_qps_in_use - qps)

    # ------------------------------------------------------------- read

    def fits_in(self, quota: ResourceQuota, *, cpu: float, storage: float, qps: float) -> bool:
        with self._lock:
            return (
                self.cpu_cores_in_use + cpu <= quota.cpu_cores
                and self.storage_gb_in_use + storage <= quota.storage_gb
                and self.ingestion_qps_in_use + qps <= quota.ingestion_qps
            )


__all__ = ["ResourceQuota", "ResourceUsage"]
