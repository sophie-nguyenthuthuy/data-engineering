"""`/api/v1/analytics/top-errors` unit test, ClickHouse mocked out."""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from api import main as api_main


def _patch(monkeypatch, rows: list[dict]) -> None:
    mock_result = MagicMock()
    mock_client = MagicMock()
    mock_client.query.return_value = mock_result
    monkeypatch.setattr(api_main, "get_client", lambda _cfg=None: mock_client)
    monkeypatch.setattr(api_main, "rows_to_dicts", lambda _r: rows)
    monkeypatch.setattr(
        api_main,
        "Config",
        type("C", (), {"from_env": staticmethod(lambda: object())}),
    )


def test_top_errors_maps_rows(monkeypatch):
    _patch(
        monkeypatch,
        [
            {
                "event_type": "checkout",
                "country": "US",
                "device": "mobile",
                "events": 1000,
                "errors": 42,
                "error_rate": 0.042,
            },
            {
                "event_type": "login",
                "country": "DE",
                "device": "desktop",
                "events": 800,
                "errors": 10,
                "error_rate": 0.0125,
            },
        ],
    )

    client = TestClient(api_main.app)
    r = client.get("/api/v1/analytics/top-errors?hours=6&limit=5")
    assert r.status_code == 200

    body = r.json()
    assert len(body) == 2
    assert body[0]["event_type"] == "checkout"
    assert body[0]["errors"] == 42
    assert body[1]["error_rate"] == 0.0125


def test_top_errors_rejects_out_of_range(monkeypatch):
    _patch(monkeypatch, [])
    client = TestClient(api_main.app)

    # hours > 168 (one week cap)
    assert client.get("/api/v1/analytics/top-errors?hours=500").status_code == 422
    # limit > 100
    assert client.get("/api/v1/analytics/top-errors?limit=9999").status_code == 422
