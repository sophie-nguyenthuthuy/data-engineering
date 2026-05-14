"""Platform facade — wires the registries, isolation, admission, scheduler."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from mtip.admission import AdmissionController, AdmissionRequest
from mtip.isolation.compute import ComputeSlots
from mtip.quota import ResourceUsage
from mtip.registry.source import SourceRegistry, SourceSpec
from mtip.registry.tenant import Tenant, TenantRegistry
from mtip.scheduler import FairScheduler, Job

if TYPE_CHECKING:
    from mtip.admission import Decision


@dataclass
class Platform:
    """Single-process platform facade."""

    tenants: TenantRegistry = field(default_factory=TenantRegistry)
    sources: SourceRegistry = field(default_factory=SourceRegistry)
    slots: ComputeSlots = field(default_factory=lambda: ComputeSlots(total=4))
    usage: dict[str, ResourceUsage] = field(default_factory=dict)
    scheduler: FairScheduler = field(default_factory=FairScheduler)

    @property
    def admission(self) -> AdmissionController:
        return AdmissionController(tenants=self.tenants, usage=self.usage)

    # ----------------------------------------------------------- tenants

    def register_tenant(self, tenant: Tenant) -> None:
        self.tenants.register(tenant)
        self.usage.setdefault(tenant.id, ResourceUsage())
        self.scheduler.add_tenant(tenant)

    def register_source(
        self, tenant_id: str, source_id: str, kind: str, config: dict[str, Any]
    ) -> SourceSpec:
        if tenant_id not in self.tenants:
            raise KeyError(f"unknown tenant {tenant_id!r}")
        spec = SourceSpec(tenant_id=tenant_id, source_id=source_id, kind=kind, config=dict(config))
        self.sources.register(spec)
        return spec

    # -------------------------------------------------------------- jobs

    def submit_job(
        self,
        tenant_id: str,
        job_id: str,
        *,
        cpu: float,
        storage_gb: float = 0.0,
        qps: float = 0.0,
    ) -> Decision:
        decision = self.admission.evaluate(
            AdmissionRequest(tenant_id=tenant_id, cpu=cpu, storage_gb=storage_gb, qps=qps)
        )
        from mtip.admission import Decision as _D

        if decision is not _D.ADMIT:
            return decision
        self.usage[tenant_id].reserve(cpu=cpu, storage=storage_gb, qps=qps)
        self.scheduler.submit(Job(tenant_id=tenant_id, job_id=job_id, cost=cpu))
        return decision

    def release(
        self,
        tenant_id: str,
        *,
        cpu: float = 0.0,
        storage_gb: float = 0.0,
        qps: float = 0.0,
    ) -> None:
        if tenant_id in self.usage:
            self.usage[tenant_id].release(cpu=cpu, storage=storage_gb, qps=qps)


__all__ = ["Platform"]
