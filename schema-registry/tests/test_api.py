import pytest
import pytest_asyncio
import httpx
from fastapi.testclient import TestClient
from src.api.app import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_register_and_list(client):
    schema = {
        "type": "object",
        "properties": {"id": {"type": "integer"}, "email": {"type": "string"}},
        "required": ["id"],
    }
    r = client.post("/api/v1/subjects/users/versions", json={"schema_definition": schema})
    assert r.status_code == 201
    sv = r.json()
    assert sv["version"] == 1

    r2 = client.get("/api/v1/subjects/users/versions")
    assert 1 in r2.json()


def test_idempotent_registration(client):
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]}
    r1 = client.post("/api/v1/subjects/users/versions", json={"schema_definition": schema})
    r2 = client.post("/api/v1/subjects/users/versions", json={"schema_definition": schema})
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json()["version"] == r2.json()["version"]


def test_compatibility_check(client):
    schema_v1 = {
        "type": "object",
        "properties": {"id": {"type": "integer"}},
        "required": ["id"],
    }
    client.post("/api/v1/subjects/orders/versions", json={"schema_definition": schema_v1})

    # backward compatible (add optional field)
    schema_v2 = {
        "type": "object",
        "properties": {"id": {"type": "integer"}, "note": {"type": "string", "default": ""}},
        "required": ["id"],
    }
    r = client.post(
        "/api/v1/compatibility/subjects/orders/versions",
        json={"schema_definition": schema_v2},
    )
    assert r.json()["compatible"] is True


def test_compatibility_rejects_breaking(client):
    schema_v1 = {
        "type": "object",
        "properties": {"id": {"type": "integer"}},
        "required": ["id"],
    }
    client.post("/api/v1/subjects/products/versions", json={"schema_definition": schema_v1})

    breaking = {
        "type": "object",
        "properties": {"id": {"type": "integer"}, "mandatory": {"type": "string"}},
        "required": ["id", "mandatory"],
    }
    r = client.post(
        "/api/v1/compatibility/subjects/products/versions",
        json={"schema_definition": breaking},
    )
    assert r.json()["compatible"] is False


def test_generate_migration(client):
    v1 = {"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}, "required": ["id"]}
    v2 = {"type": "object", "properties": {"id": {"type": "integer"}, "full_name": {"type": "string"}}, "required": ["id"]}

    client.put("/api/v1/config/events", json={"compatibility": "NONE"})
    client.post("/api/v1/subjects/events/versions", json={"schema_definition": v1})
    client.post("/api/v1/subjects/events/versions", json={"schema_definition": v2})

    r = client.post("/api/v1/subjects/events/migrations/generate/1/2")
    assert r.status_code == 200
    script = r.json()
    assert script["from_version"] == 1
    assert script["to_version"] == 2
    assert len(script["steps"]) > 0


def test_replay_endpoint(client):
    v1 = {"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}, "required": ["id"]}
    v2 = {"type": "object", "properties": {"id": {"type": "integer"}, "full_name": {"type": "string"}}, "required": ["id"]}

    client.put("/api/v1/config/replay_test", json={"compatibility": "NONE"})
    client.post("/api/v1/subjects/replay_test/versions", json={"schema_definition": v1})
    client.post("/api/v1/subjects/replay_test/versions", json={"schema_definition": v2})

    events = [
        {"event_id": "e1", "schema_version": 1, "payload": {"id": 1, "name": "Alice"}},
        {"event_id": "e2", "schema_version": 1, "payload": {"id": 2, "name": "Bob"}},
    ]
    r = client.post(
        "/api/v1/subjects/replay_test/replay",
        json={"events": events, "target_version": 2, "validate": False},
    )
    assert r.status_code == 200
    result = r.json()
    assert result["total"] == 2
    assert result["succeeded"] == 2
