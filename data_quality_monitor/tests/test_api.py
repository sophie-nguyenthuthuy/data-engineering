"""Tests for the FastAPI dashboard endpoints."""
from __future__ import annotations
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport

from src.dashboard.api import create_app
from src.models import ValidationStatus


@pytest.fixture
def mock_repo():
    repo = AsyncMock()
    repo.get_recent_results = AsyncMock(return_value=[])
    repo.get_failure_summary = AsyncMock(return_value=[])
    repo.get_pass_rate_last_hour = AsyncMock(return_value=0.98)
    return repo


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.keys = AsyncMock(return_value=[])
    redis.hgetall = AsyncMock(return_value={})
    redis.exists = AsyncMock(return_value=0)
    redis.delete = AsyncMock(return_value=1)
    redis.pubsub = MagicMock(return_value=AsyncMock())
    return redis


@pytest.fixture
def mock_job_ctrl():
    ctrl = AsyncMock()
    ctrl.list_active_blocks = AsyncMock(return_value=[])
    ctrl.is_blocked = AsyncMock(return_value=False)
    ctrl.force_unblock = AsyncMock(return_value=True)
    return ctrl


@pytest.fixture
def mock_collector():
    return MagicMock()


@pytest.fixture
def app(mock_repo, mock_redis, mock_job_ctrl, mock_collector):
    with patch("src.dashboard.api.redis_subscription_loop", new_callable=AsyncMock):
        return create_app(mock_repo, mock_redis, mock_job_ctrl, mock_collector)


@pytest.fixture
async def client(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
class TestHealthEndpoint:
    async def test_health_ok(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
class TestResultsEndpoint:
    async def test_list_results_empty(self, client, mock_repo):
        mock_repo.get_recent_results = AsyncMock(return_value=[])
        resp = await client.get("/api/v1/results")
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_list_results_with_table_filter(self, client, mock_repo):
        resp = await client.get("/api/v1/results?table=orders&limit=10")
        assert resp.status_code == 200
        mock_repo.get_recent_results.assert_called_with(limit=10, table_name="orders")

    async def test_get_result_not_found(self, client, mock_redis):
        mock_redis.get = AsyncMock(return_value=None)
        resp = await client.get("/api/v1/results/nonexistent-id")
        assert resp.status_code == 404


@pytest.mark.asyncio
class TestBlocksEndpoint:
    async def test_list_blocks_empty(self, client, mock_job_ctrl):
        mock_job_ctrl.list_active_blocks = AsyncMock(return_value=[])
        resp = await client.get("/api/v1/blocks")
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_block_status_not_blocked(self, client, mock_job_ctrl):
        mock_job_ctrl.is_blocked = AsyncMock(return_value=False)
        resp = await client.get("/api/v1/blocks/etl_transform/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["blocked"] is False

    async def test_unblock_job(self, client, mock_job_ctrl):
        mock_job_ctrl.force_unblock = AsyncMock(return_value=True)
        resp = await client.delete("/api/v1/blocks/etl_transform")
        assert resp.status_code == 200
        assert "lifted" in resp.json()["message"]

    async def test_unblock_nonexistent_job_404(self, client, mock_job_ctrl):
        mock_job_ctrl.force_unblock = AsyncMock(return_value=False)
        resp = await client.delete("/api/v1/blocks/ghost_job")
        assert resp.status_code == 404


@pytest.mark.asyncio
class TestSnapshotEndpoint:
    async def test_snapshot_no_data(self, client, mock_redis):
        mock_redis.get = AsyncMock(return_value=None)
        resp = await client.get("/api/v1/snapshot")
        assert resp.status_code == 200
        assert "message" in resp.json()

    async def test_snapshot_with_data(self, client, mock_redis):
        fake = {"overall_pass_rate": 0.97, "total_batches_last_hour": 42}
        mock_redis.get = AsyncMock(return_value=json.dumps(fake).encode())
        resp = await client.get("/api/v1/snapshot")
        assert resp.status_code == 200
        assert resp.json()["overall_pass_rate"] == 0.97
