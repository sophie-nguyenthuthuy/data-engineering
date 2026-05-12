"""Serving API integration tests — uses ASGI test client, no real network.

Strategy: patch the *constructors* of AsyncOnlineStore, OfflineStore, and
FeatureRegistry inside the server module so the lifespan creates mocks rather
than real instances that try to connect to Redis/disk.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest
from fastapi.testclient import TestClient

import feature_store.serving.server as server_module
from feature_store.serving.server import app


def _make_mock_online():
    m = AsyncMock()
    m.healthcheck.return_value = True
    m.get.return_value = {"score": 0.9, "count": 42}
    m.get_multi_group.return_value = {
        ("users", "u1"): {"score": 0.9},
        ("items", "i1"): {"pop": 0.7},
    }
    m.close.return_value = None
    return m


def _make_mock_offline():
    m = MagicMock()
    m.get_stats.return_value = {"row_count": 100, "entity_count": 50}
    return m


def _make_mock_registry():
    m = MagicMock()
    m.list_groups.return_value = ["users", "items"]
    m.to_json.return_value = '{"users": {}}'
    return m


@pytest.fixture()
def mock_stores():
    """
    Patch class constructors so the FastAPI lifespan produces mocks
    instead of real store instances.  Yields (online, offline, registry).
    """
    mock_online = _make_mock_online()
    mock_offline = _make_mock_offline()
    mock_registry = _make_mock_registry()

    # Patch constructors in the server module's namespace
    with (
        patch("feature_store.serving.server.AsyncOnlineStore", return_value=mock_online),
        patch("feature_store.serving.server.OfflineStore", return_value=mock_offline),
        patch("feature_store.serving.server.FeatureRegistry", return_value=mock_registry),
        patch("os.path.exists", return_value=False),   # skip register_from_config
    ):
        server_module._L1_CACHE.clear()
        yield mock_online, mock_offline, mock_registry


@pytest.fixture()
def client(mock_stores):
    """TestClient created AFTER constructor patches are in place."""
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c


# ------------------------------------------------------------------ #
# Tests                                                               #
# ------------------------------------------------------------------ #

class TestGetFeatures:
    def test_single_get_200(self, client, mock_stores):
        resp = client.get("/features/users/u1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["group"] == "users"
        assert data["entity_id"] == "u1"
        assert "score" in data["features"]

    def test_missing_entity_404(self, client, mock_stores):
        mock_online, *_ = mock_stores
        mock_online.get.return_value = None
        resp = client.get("/features/users/unknown")
        assert resp.status_code == 404

    def test_l1_cache_hit(self, client, mock_stores):
        """Second identical request must be served from L1, not Redis."""
        mock_online, *_ = mock_stores
        server_module._L1_CACHE.clear()
        client.get("/features/users/u1")   # populates L1
        client.get("/features/users/u1")   # must hit L1
        assert mock_online.get.call_count == 1


class TestBatchGet:
    def test_batch_200(self, client, mock_stores):
        resp = client.post(
            "/features/batch",
            json={"requests": [
                {"group": "users", "entity_id": "u1"},
                {"group": "items", "entity_id": "i1"},
            ]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["results"]) == 2
        assert "latency_ms" in data

    def test_batch_latency_reported(self, client, mock_stores):
        resp = client.post(
            "/features/batch",
            json={"requests": [{"group": "users", "entity_id": "u1"}]},
        )
        assert resp.status_code == 200
        assert resp.json()["latency_ms"] >= 0

    def test_batch_l1_partial_hit(self, client, mock_stores):
        """Entities already in L1 must not trigger an extra Redis call."""
        mock_online, *_ = mock_stores
        server_module._L1_CACHE.clear()
        # Pre-warm L1 for u1
        client.get("/features/users/u1")
        call_count_before = mock_online.get_multi_group.call_count
        # Batch: u1 (L1 hit) + i1 (miss → Redis pipeline)
        client.post(
            "/features/batch",
            json={"requests": [
                {"group": "users", "entity_id": "u1"},
                {"group": "items", "entity_id": "i1"},
            ]},
        )
        # get_multi_group called once for the miss only
        assert mock_online.get_multi_group.call_count == call_count_before + 1


class TestHealth:
    def test_health_ok(self, client, mock_stores):
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["redis"] is True

    def test_health_degraded(self, client, mock_stores):
        mock_online, *_ = mock_stores
        mock_online.healthcheck.return_value = False
        resp = client.get("/health")
        assert resp.json()["status"] == "degraded"


class TestRegistry:
    def test_registry_endpoint(self, client, mock_stores):
        resp = client.get("/registry")
        assert resp.status_code == 200
        assert "users" in resp.json()


class TestMetrics:
    def test_prometheus_endpoint(self, client, mock_stores):
        client.get("/features/users/u1")
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert b"fs_request_duration_seconds" in resp.content
