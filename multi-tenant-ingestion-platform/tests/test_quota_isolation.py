"""Quota + isolation tests."""

from __future__ import annotations

import pytest

from mtip.isolation.compute import ComputeSlots
from mtip.isolation.storage import StorageNamespace
from mtip.quota import ResourceQuota, ResourceUsage

# --------------------------------------------------------------- Quota


def test_resource_quota_rejects_non_positive():
    with pytest.raises(ValueError):
        ResourceQuota(cpu_cores=0, storage_gb=1, ingestion_qps=1)
    with pytest.raises(ValueError):
        ResourceQuota(cpu_cores=1, storage_gb=0, ingestion_qps=1)
    with pytest.raises(ValueError):
        ResourceQuota(cpu_cores=1, storage_gb=1, ingestion_qps=0)


def test_resource_usage_reserve_release():
    u = ResourceUsage()
    u.reserve(cpu=1, storage=2, qps=3)
    assert u.cpu_cores_in_use == 1 and u.storage_gb_in_use == 2 and u.ingestion_qps_in_use == 3
    u.release(cpu=1, storage=2, qps=3)
    assert u.cpu_cores_in_use == 0 and u.storage_gb_in_use == 0 and u.ingestion_qps_in_use == 0


def test_resource_usage_release_clamps_at_zero():
    u = ResourceUsage()
    u.release(cpu=5)
    assert u.cpu_cores_in_use == 0


def test_resource_usage_reject_negative():
    u = ResourceUsage()
    with pytest.raises(ValueError):
        u.reserve(cpu=-1)
    with pytest.raises(ValueError):
        u.release(storage=-2)


def test_resource_usage_fits_in():
    q = ResourceQuota(cpu_cores=2, storage_gb=10, ingestion_qps=100)
    u = ResourceUsage()
    u.reserve(cpu=1, storage=5, qps=50)
    assert u.fits_in(q, cpu=1, storage=5, qps=50)
    assert not u.fits_in(q, cpu=1.01, storage=0, qps=0)
    assert not u.fits_in(q, cpu=0, storage=5.01, qps=0)
    assert not u.fits_in(q, cpu=0, storage=0, qps=50.01)


# --------------------------------------------------------- Storage namespace


def test_storage_namespace_validates_tenant_id():
    with pytest.raises(ValueError):
        StorageNamespace(tenant_id="Bad")


def test_storage_namespace_base_and_resolve():
    ns = StorageNamespace(tenant_id="team-a")
    assert ns.base == "tenants/team-a"
    assert (
        ns.resolve("orders/2026/05/13/file.jsonl") == "tenants/team-a/orders/2026/05/13/file.jsonl"
    )


def test_storage_namespace_rejects_absolute_path():
    ns = StorageNamespace(tenant_id="team-a")
    with pytest.raises(ValueError):
        ns.resolve("/absolute")


def test_storage_namespace_rejects_traversal():
    ns = StorageNamespace(tenant_id="team-a")
    with pytest.raises(ValueError):
        ns.resolve("orders/../../etc/passwd")
    with pytest.raises(ValueError):
        ns.resolve("./orders")


def test_storage_namespace_rejects_empty_path():
    ns = StorageNamespace(tenant_id="team-a")
    with pytest.raises(ValueError):
        ns.resolve("")


# ----------------------------------------------------------- Compute slots


def test_compute_slots_rejects_zero_total():
    with pytest.raises(ValueError):
        ComputeSlots(total=0)


def test_compute_slots_acquire_and_release():
    s = ComputeSlots(total=4)
    assert s.acquire("a", 2)
    assert s.free() == 2
    assert s.in_use_for("a") == 2
    s.release("a", 2)
    assert s.free() == 4
    assert s.in_use_for("a") == 0


def test_compute_slots_acquire_returns_false_when_full():
    s = ComputeSlots(total=2)
    assert s.acquire("a", 2)
    assert not s.acquire("b", 1)


def test_compute_slots_rejects_zero_n_and_empty_tenant():
    s = ComputeSlots(total=4)
    with pytest.raises(ValueError):
        s.acquire("a", 0)
    with pytest.raises(ValueError):
        s.acquire("", 1)
    with pytest.raises(ValueError):
        s.release("a", 0)


def test_compute_slots_release_underflow_clamps_to_zero():
    s = ComputeSlots(total=4)
    s.acquire("a", 1)
    s.release("a", 10)
    assert s.in_use_for("a") == 0
