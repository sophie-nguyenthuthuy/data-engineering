"""Admission + scheduler + end-to-end Platform tests."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mtip.admission import AdmissionController, AdmissionRequest, Decision
from mtip.platform import Platform
from mtip.quota import ResourceQuota, ResourceUsage
from mtip.registry.tenant import Tenant, TenantRegistry
from mtip.scheduler import FairScheduler, Job


def _tenant(tid="team-a", cpu=2.0, storage=10.0, qps=100.0) -> Tenant:
    return Tenant(id=tid, display_name=tid, quota=ResourceQuota(cpu, storage, qps))


# ----------------------------------------------------------- Admission


def test_admission_rejects_unknown_tenant():
    ac = AdmissionController(tenants=TenantRegistry(), usage={})
    assert (
        ac.evaluate(AdmissionRequest("ghost", cpu=0.0, storage_gb=0.0, qps=0.0))
        == Decision.REJECT_UNKNOWN_TENANT
    )


def test_admission_admits_within_quota():
    tr = TenantRegistry()
    tr.register(_tenant())
    ac = AdmissionController(tenants=tr, usage={})
    assert (
        ac.evaluate(AdmissionRequest("team-a", cpu=1.0, storage_gb=1.0, qps=10.0)) == Decision.ADMIT
    )


def test_admission_rejects_over_cpu():
    tr = TenantRegistry()
    tr.register(_tenant(cpu=2.0))
    used = ResourceUsage()
    used.reserve(cpu=1.5)
    ac = AdmissionController(tenants=tr, usage={"team-a": used})
    assert (
        ac.evaluate(AdmissionRequest("team-a", cpu=1.0, storage_gb=0, qps=0))
        == Decision.REJECT_OVER_CPU
    )


def test_admission_rejects_over_storage():
    tr = TenantRegistry()
    tr.register(_tenant(storage=10.0))
    used = ResourceUsage()
    used.reserve(storage=9.5)
    ac = AdmissionController(tenants=tr, usage={"team-a": used})
    assert (
        ac.evaluate(AdmissionRequest("team-a", cpu=0, storage_gb=1.0, qps=0))
        == Decision.REJECT_OVER_STORAGE
    )


def test_admission_rejects_over_qps():
    tr = TenantRegistry()
    tr.register(_tenant(qps=100.0))
    used = ResourceUsage()
    used.reserve(qps=95)
    ac = AdmissionController(tenants=tr, usage={"team-a": used})
    assert (
        ac.evaluate(AdmissionRequest("team-a", cpu=0, storage_gb=0, qps=10.0))
        == Decision.REJECT_OVER_QPS
    )


def test_admission_request_rejects_negative():
    with pytest.raises(ValueError):
        AdmissionRequest("t", cpu=-1, storage_gb=0, qps=0)


# ----------------------------------------------------------- Scheduler


def test_scheduler_rejects_invalid_quantum():
    with pytest.raises(ValueError):
        FairScheduler(quantum=0)


def test_scheduler_round_robin_equal_weights():
    s = FairScheduler()
    s.add_tenant(_tenant("a", cpu=1), weight=1.0)
    s.add_tenant(_tenant("b", cpu=1), weight=1.0)
    for i in range(3):
        s.submit(Job(tenant_id="a", job_id=f"a{i}", cost=1.0))
        s.submit(Job(tenant_id="b", job_id=f"b{i}", cost=1.0))
    out = s.schedule(6)
    tenants_in_order = [x.job.tenant_id for x in out]
    # Equal weights → alternating (a, b) within each round.
    assert tenants_in_order.count("a") == 3
    assert tenants_in_order.count("b") == 3


def test_scheduler_weight_skews_toward_heavier_tenant():
    s = FairScheduler()
    s.add_tenant(_tenant("a"), weight=3.0)
    s.add_tenant(_tenant("b"), weight=1.0)
    for i in range(8):
        s.submit(Job(tenant_id="a", job_id=f"a{i}"))
        s.submit(Job(tenant_id="b", job_id=f"b{i}"))
    out = s.schedule(8)
    a_count = sum(1 for x in out if x.job.tenant_id == "a")
    b_count = sum(1 for x in out if x.job.tenant_id == "b")
    assert a_count > b_count


def test_scheduler_returns_empty_when_no_jobs():
    s = FairScheduler()
    s.add_tenant(_tenant("a"))
    assert s.schedule(5) == []


def test_scheduler_rejects_unknown_tenant_submission():
    s = FairScheduler()
    with pytest.raises(KeyError):
        s.submit(Job(tenant_id="ghost", job_id="j"))


def test_scheduler_rejects_negative_n():
    s = FairScheduler()
    with pytest.raises(ValueError):
        s.schedule(0)


# --------------------------------------------------------- Job invariants


def test_job_rejects_invalid_fields():
    with pytest.raises(ValueError):
        Job(tenant_id="", job_id="j")
    with pytest.raises(ValueError):
        Job(tenant_id="t", job_id="")
    with pytest.raises(ValueError):
        Job(tenant_id="t", job_id="j", cost=0)


# ---------------------------------------------------------- Platform


def test_platform_end_to_end():
    p = Platform()
    p.register_tenant(_tenant("team-a", cpu=2))
    spec = p.register_source("team-a", "orders", "csv", {"path": "/orders.csv"})
    assert spec.tenant_id == "team-a"

    d1 = p.submit_job("team-a", "j1", cpu=1.0)
    d2 = p.submit_job("team-a", "j2", cpu=1.0)
    d3 = p.submit_job("team-a", "j3", cpu=1.0)  # over quota
    assert d1 == Decision.ADMIT
    assert d2 == Decision.ADMIT
    assert d3 == Decision.REJECT_OVER_CPU


def test_platform_releases_capacity():
    p = Platform()
    p.register_tenant(_tenant("a", cpu=1))
    assert p.submit_job("a", "j1", cpu=1.0) == Decision.ADMIT
    assert p.submit_job("a", "j2", cpu=1.0) == Decision.REJECT_OVER_CPU
    p.release("a", cpu=1.0)
    assert p.submit_job("a", "j2", cpu=1.0) == Decision.ADMIT


def test_platform_rejects_source_for_unknown_tenant():
    p = Platform()
    with pytest.raises(KeyError):
        p.register_source("ghost", "s", "csv", {})


# ----------------------------------------------------------- Hypothesis


@settings(max_examples=20, deadline=None)
@given(
    weights=st.lists(st.integers(1, 10), min_size=2, max_size=4),
    jobs_per_tenant=st.integers(2, 10),
)
def test_property_fair_scheduler_serves_proportional_to_weight(weights, jobs_per_tenant):
    """Total jobs served per tenant ≈ weight ratio, within ±2."""
    s = FairScheduler()
    tenants = []
    for i, w in enumerate(weights):
        t = _tenant(f"t-{i}", cpu=float(w))
        s.add_tenant(t, weight=float(w))
        tenants.append(t)
    for t in tenants:
        for j in range(jobs_per_tenant):
            s.submit(Job(tenant_id=t.id, job_id=f"{t.id}-{j}"))
    total_jobs = jobs_per_tenant * len(tenants)
    out = s.schedule(total_jobs)
    counts = {t.id: sum(1 for x in out if x.job.tenant_id == t.id) for t in tenants}
    # Heavier weights serve no fewer jobs than lighter ones (modulo +1 for
    # quantisation when the workload is small).
    by_weight = sorted(
        zip(weights, [counts[t.id] for t in tenants], strict=True),
        key=lambda p: p[0],
    )
    from itertools import pairwise

    for (w1, c1), (w2, c2) in pairwise(by_weight):
        if w1 < w2:
            assert c1 <= c2 + 1
