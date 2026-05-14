"""multi-tenant-ingestion-platform — quota-bounded multi-tenant ingestion."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from mtip.admission import AdmissionController, Decision
    from mtip.isolation.compute import ComputeSlots
    from mtip.isolation.storage import StorageNamespace
    from mtip.platform import Platform
    from mtip.quota import ResourceQuota, ResourceUsage
    from mtip.registry.source import SourceRegistry, SourceSpec
    from mtip.registry.tenant import Tenant, TenantRegistry
    from mtip.scheduler import FairScheduler, Job, Scheduled

_LAZY: dict[str, tuple[str, str]] = {
    "Tenant": ("mtip.registry.tenant", "Tenant"),
    "TenantRegistry": ("mtip.registry.tenant", "TenantRegistry"),
    "ResourceQuota": ("mtip.quota", "ResourceQuota"),
    "ResourceUsage": ("mtip.quota", "ResourceUsage"),
    "SourceSpec": ("mtip.registry.source", "SourceSpec"),
    "SourceRegistry": ("mtip.registry.source", "SourceRegistry"),
    "StorageNamespace": ("mtip.isolation.storage", "StorageNamespace"),
    "ComputeSlots": ("mtip.isolation.compute", "ComputeSlots"),
    "AdmissionController": ("mtip.admission", "AdmissionController"),
    "Decision": ("mtip.admission", "Decision"),
    "Job": ("mtip.scheduler", "Job"),
    "Scheduled": ("mtip.scheduler", "Scheduled"),
    "FairScheduler": ("mtip.scheduler", "FairScheduler"),
    "Platform": ("mtip.platform", "Platform"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AdmissionController",
    "ComputeSlots",
    "Decision",
    "FairScheduler",
    "Job",
    "Platform",
    "ResourceQuota",
    "ResourceUsage",
    "Scheduled",
    "SourceRegistry",
    "SourceSpec",
    "StorageNamespace",
    "Tenant",
    "TenantRegistry",
    "__version__",
]
