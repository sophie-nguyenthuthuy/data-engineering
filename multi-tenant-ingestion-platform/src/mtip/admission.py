"""Admission controller.

Consults the tenant's quota + live usage and returns an admit/reject
decision. The controller does not actually reserve resources — that's
the scheduler's job; we only encode the policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mtip.quota import ResourceUsage
    from mtip.registry.tenant import TenantRegistry


class Decision(str, Enum):
    """Admission outcome."""

    ADMIT = "admit"
    REJECT_UNKNOWN_TENANT = "reject_unknown_tenant"
    REJECT_OVER_CPU = "reject_over_cpu"
    REJECT_OVER_STORAGE = "reject_over_storage"
    REJECT_OVER_QPS = "reject_over_qps"


@dataclass(frozen=True, slots=True)
class AdmissionRequest:
    tenant_id: str
    cpu: float
    storage_gb: float
    qps: float

    def __post_init__(self) -> None:
        if self.cpu < 0 or self.storage_gb < 0 or self.qps < 0:
            raise ValueError("admission request fields must be non-negative")


@dataclass
class AdmissionController:
    """Checks tenant quotas against live usage."""

    tenants: TenantRegistry
    usage: dict[str, ResourceUsage]

    def evaluate(self, request: AdmissionRequest) -> Decision:
        if request.tenant_id not in self.tenants:
            return Decision.REJECT_UNKNOWN_TENANT
        tenant = self.tenants.get(request.tenant_id)
        used = self.usage.setdefault(request.tenant_id, _zero_usage())
        if used.cpu_cores_in_use + request.cpu > tenant.quota.cpu_cores:
            return Decision.REJECT_OVER_CPU
        if used.storage_gb_in_use + request.storage_gb > tenant.quota.storage_gb:
            return Decision.REJECT_OVER_STORAGE
        if used.ingestion_qps_in_use + request.qps > tenant.quota.ingestion_qps:
            return Decision.REJECT_OVER_QPS
        return Decision.ADMIT


def _zero_usage() -> ResourceUsage:
    from mtip.quota import ResourceUsage

    return ResourceUsage()


__all__ = ["AdmissionController", "AdmissionRequest", "Decision"]
