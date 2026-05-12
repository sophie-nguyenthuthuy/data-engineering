"""Health endpoint smoke test. ClickHouse is monkeypatched so the test is offline."""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from api import main as api_main


def test_healthz_ok(monkeypatch):
    mock_client = MagicMock()
    mock_client.query.return_value = None
    monkeypatch.setattr(api_main, "get_client", lambda _cfg=None: mock_client)
    monkeypatch.setattr(
        api_main,
        "Config",
        type("C", (), {"from_env": staticmethod(lambda: object())}),
    )

    client = TestClient(api_main.app)
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "clickhouse": "ok"}


def test_healthz_degraded_on_error(monkeypatch):
    def boom(_cfg=None):
        raise RuntimeError("clickhouse down")

    monkeypatch.setattr(api_main, "get_client", boom)
    monkeypatch.setattr(
        api_main,
        "Config",
        type("C", (), {"from_env": staticmethod(lambda: object())}),
    )

    client = TestClient(api_main.app)
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "degraded"
    assert r.json()["clickhouse"] == "error"
