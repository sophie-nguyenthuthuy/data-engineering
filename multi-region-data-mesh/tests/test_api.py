"""
End-to-end API tests using FastAPI's async test client.
Runs a single-node instance (no peers) to exercise the full HTTP layer.

Note: ASGITransport does not trigger FastAPI lifespan, so we inject
store + engine directly into app.state in the fixture.
"""
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

import os
os.environ["MESH_REGION_ID"] = "test-region"
os.environ["MESH_PEER_URLS"] = ""

from src.main import app
from src.store.database import RegionalStore
from src.replication.engine import ReplicationEngine


@pytest_asyncio.fixture
async def client(tmp_path):
    db_path = str(tmp_path / "test_api.db")
    store = RegionalStore(db_path)
    await store.open()

    engine = ReplicationEngine(
        store=store,
        region_id="test-region",
        conflict_strategy="lww",
    )
    await engine.start()

    app.state.store = store
    app.state.engine = engine

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c

    await engine.stop()
    await store.close()


@pytest.mark.asyncio
async def test_root(client):
    r = await client.get("/")
    assert r.status_code == 200
    data = r.json()
    assert data["region"] == "test-region"


@pytest.mark.asyncio
async def test_ping(client):
    r = await client.get("/ping")
    assert r.status_code == 200
    assert r.json()["region"] == "test-region"


@pytest.mark.asyncio
async def test_create_account(client):
    r = await client.post("/accounts", json={"owner": "Alice", "balance": 500.0})
    assert r.status_code == 201
    body = r.json()
    assert body["owner"] == "Alice"
    assert body["balance"] == 500.0
    assert body["account_id"]
    assert body["origin_region"] == "test-region"


@pytest.mark.asyncio
async def test_get_account(client):
    create = await client.post("/accounts", json={"owner": "Bob", "balance": 200.0})
    acc_id = create.json()["account_id"]
    r = await client.get(f"/accounts/{acc_id}")
    assert r.status_code == 200
    assert r.json()["owner"] == "Bob"


@pytest.mark.asyncio
async def test_get_account_not_found(client):
    r = await client.get("/accounts/does-not-exist")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_list_accounts(client):
    await client.post("/accounts", json={"owner": "C1", "balance": 10.0})
    await client.post("/accounts", json={"owner": "C2", "balance": 20.0})
    r = await client.get("/accounts")
    assert r.status_code == 200
    assert len(r.json()) >= 2


@pytest.mark.asyncio
async def test_update_balance_credit(client):
    create = await client.post("/accounts", json={"owner": "Dave", "balance": 100.0})
    acc_id = create.json()["account_id"]
    r = await client.patch(f"/accounts/{acc_id}/balance", json={"delta": 50.0, "note": "bonus"})
    assert r.status_code == 200
    assert r.json()["balance"] == pytest.approx(150.0)
    assert r.json()["metadata"].get("last_note") == "bonus"


@pytest.mark.asyncio
async def test_update_balance_debit(client):
    create = await client.post("/accounts", json={"owner": "Eve", "balance": 200.0})
    acc_id = create.json()["account_id"]
    r = await client.patch(f"/accounts/{acc_id}/balance", json={"delta": -80.0})
    assert r.status_code == 200
    assert r.json()["balance"] == pytest.approx(120.0)


@pytest.mark.asyncio
async def test_health_endpoint(client):
    r = await client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["region_id"] == "test-region"
    assert body["status"] in ("healthy", "degraded")
    assert "total_accounts" in body
    assert "conflicts_resolved" in body
    assert "uptime_seconds" in body


@pytest.mark.asyncio
async def test_internal_export_records(client):
    await client.post("/accounts", json={"owner": "Faye", "balance": 300.0})
    r = await client.get("/internal/records?since=0")
    assert r.status_code == 200
    body = r.json()
    assert body["source_region"] == "test-region"
    assert len(body["records"]) >= 1


@pytest.mark.asyncio
async def test_dashboard_returns_html(client):
    r = await client.get("/dashboard")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    assert "Data Mesh" in r.text
