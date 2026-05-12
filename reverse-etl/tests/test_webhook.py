import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from src.triggers.webhook import create_webhook_app
from src.models import PipelineConfig, RunResult


def _make_pipeline(name="test_pipe", enabled=True):
    return PipelineConfig(
        name=name,
        enabled=enabled,
        source={"type": "postgres", "query": "SELECT 1"},
        destination={"type": "slack", "params": {"channel": "#x"}},
    )


@pytest.fixture
def client():
    pipelines = [_make_pipeline("pipe_a"), _make_pipeline("pipe_b", enabled=False)]
    app = create_webhook_app(pipelines)
    return TestClient(app)


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_list_pipelines(client):
    resp = client.get("/pipelines")
    assert resp.status_code == 200
    names = [p["name"] for p in resp.json()]
    assert "pipe_a" in names
    assert "pipe_b" in names


def test_trigger_success(client):
    ok_result = RunResult(pipeline="pipe_a", success=True, rows_extracted=5, rows_synced=5)
    with patch("src.triggers.webhook.run_pipeline", return_value=ok_result):
        resp = client.post("/trigger/pipe_a")
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert data["rows_synced"] == 5


def test_trigger_not_found(client):
    resp = client.post("/trigger/does_not_exist")
    assert resp.status_code == 404


def test_trigger_disabled_pipeline(client):
    resp = client.post("/trigger/pipe_b")
    assert resp.status_code == 400
