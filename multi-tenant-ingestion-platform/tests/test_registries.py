"""Tenant + source registry tests."""

from __future__ import annotations

import pytest

from mtip.quota import ResourceQuota
from mtip.registry.source import SourceRegistry, SourceSpec
from mtip.registry.tenant import Tenant, TenantRegistry


def _q():
    return ResourceQuota(cpu_cores=2, storage_gb=10, ingestion_qps=100)


def test_tenant_validates_id_pattern():
    with pytest.raises(ValueError):
        Tenant(id="Bad-Caps", display_name="x", quota=_q())
    with pytest.raises(ValueError):
        Tenant(id="", display_name="x", quota=_q())


def test_tenant_validates_display_name():
    with pytest.raises(ValueError):
        Tenant(id="ok", display_name="", quota=_q())


def test_tenant_registry_round_trip():
    r = TenantRegistry()
    t = Tenant(id="team-a", display_name="A", quota=_q())
    r.register(t)
    assert "team-a" in r
    assert r.get("team-a") == t
    assert len(r) == 1


def test_tenant_registry_rejects_duplicate():
    r = TenantRegistry()
    t = Tenant(id="team-a", display_name="A", quota=_q())
    r.register(t)
    with pytest.raises(ValueError):
        r.register(t)


def test_tenant_registry_rejects_unknown_lookup():
    r = TenantRegistry()
    with pytest.raises(KeyError):
        r.get("nope")


def test_source_spec_validates_fields():
    with pytest.raises(ValueError):
        SourceSpec(tenant_id="", source_id="s", kind="k", config={})
    with pytest.raises(ValueError):
        SourceSpec(tenant_id="t", source_id="BAD", kind="k", config={})
    with pytest.raises(ValueError):
        SourceSpec(tenant_id="t", source_id="s", kind="", config={})


def test_source_registry_scoped_per_tenant():
    sr = SourceRegistry()
    sr.register(SourceSpec(tenant_id="a", source_id="s1", kind="csv", config={}))
    sr.register(SourceSpec(tenant_id="b", source_id="s1", kind="csv", config={}))
    assert len(sr.list_for("a")) == 1
    assert sr.list_for("a")[0].tenant_id == "a"
    assert len(sr.list_for("b")) == 1


def test_source_registry_rejects_duplicate_per_tenant():
    sr = SourceRegistry()
    sr.register(SourceSpec(tenant_id="a", source_id="s1", kind="csv", config={}))
    with pytest.raises(ValueError):
        sr.register(SourceSpec(tenant_id="a", source_id="s1", kind="csv", config={}))


def test_source_registry_list_rejects_empty_tenant():
    sr = SourceRegistry()
    with pytest.raises(ValueError):
        sr.list_for("")
