"""FastAPI read API endpoint tests."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from ssb.api.server import create_app
from ssb.manager import StateBackendManager
from ssb.topology.descriptor import OperatorDescriptor, TopologyDescriptor


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mgr_with_data() -> StateBackendManager:
    """Manager pre-loaded with topology and state data."""
    mgr = StateBackendManager(backend="memory")
    mgr.start()

    topo = TopologyDescriptor(
        version=1,
        operators={
            "word_count": OperatorDescriptor(
                operator_id="word_count",
                state_names=["count", "last_seen"],
                parallelism=1,
            ),
            "agg": OperatorDescriptor(
                operator_id="agg",
                state_names=["total"],
                parallelism=1,
            ),
        },
    )
    mgr.set_topology(topo)

    # Populate word_count::count
    for word in ["hello", "world", "foo"]:
        ctx = mgr.get_state_context("word_count", word)
        ctx.get_value_state("count", default=0).set(len(word))
        ctx.get_value_state("last_seen", default=None).set(f"2026-01-01-{word}")

    # Populate agg::total
    ctx = mgr.get_state_context("agg", "global")
    ctx.get_value_state("total", default=0).set(999)

    yield mgr
    mgr.stop()


@pytest.fixture
def client(mgr_with_data) -> TestClient:
    app = create_app(mgr_with_data)
    return TestClient(app)


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    def test_health_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "version" in data

    def test_health_has_topology_version(self, client):
        resp = client.get("/health")
        assert resp.json()["version"] == 1


# ---------------------------------------------------------------------------
# /topology
# ---------------------------------------------------------------------------


class TestTopologyEndpoints:
    def test_get_topology(self, client):
        resp = client.get("/topology")
        assert resp.status_code == 200
        data = resp.json()
        assert data["version"] == 1
        assert "word_count" in data["operators"]
        assert "agg" in data["operators"]

    def test_get_topology_operators_have_state_names(self, client):
        resp = client.get("/topology")
        wc = resp.json()["operators"]["word_count"]
        assert "count" in wc["state_names"]
        assert "last_seen" in wc["state_names"]

    def test_migrations_empty(self, client):
        resp = client.get("/topology/migrations")
        assert resp.status_code == 200
        data = resp.json()
        assert "active" in data
        assert "history" in data


# ---------------------------------------------------------------------------
# /operators
# ---------------------------------------------------------------------------


class TestOperatorsEndpoint:
    def test_list_operators(self, client):
        resp = client.get("/operators")
        assert resp.status_code == 200
        ops = resp.json()
        assert "word_count" in ops
        assert "agg" in ops

    def test_list_state_names(self, client):
        resp = client.get("/operators/word_count/state-names")
        assert resp.status_code == 200
        names = resp.json()
        assert "count" in names
        assert "last_seen" in names

    def test_unknown_operator_404(self, client):
        resp = client.get("/operators/no_such_op/state-names")
        assert resp.status_code == 404

    def test_no_topology_operators_empty(self):
        mgr = StateBackendManager(backend="memory")
        mgr.start()
        app = create_app(mgr)
        c = TestClient(app)
        resp = c.get("/operators")
        assert resp.json() == []
        mgr.stop()


# ---------------------------------------------------------------------------
# /operators/{op_id}/{state_name}/keys
# ---------------------------------------------------------------------------


class TestKeysEndpoint:
    def test_list_keys(self, client):
        resp = client.get("/operators/word_count/count/keys")
        assert resp.status_code == 200
        data = resp.json()
        assert "keys" in data
        assert len(data["keys"]) == 3
        assert "hello" in data["keys"]

    def test_limit_parameter(self, client):
        resp = client.get("/operators/word_count/count/keys?limit=2")
        data = resp.json()
        assert len(data["keys"]) <= 2

    def test_unknown_state_404(self, client):
        resp = client.get("/operators/word_count/no_such_state/keys")
        assert resp.status_code == 404

    def test_unknown_operator_404(self, client):
        resp = client.get("/operators/ghost/count/keys")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# /operators/{op_id}/{state_name}
# ---------------------------------------------------------------------------


class TestScanEndpoint:
    def test_scan_returns_entries(self, client):
        resp = client.get("/operators/word_count/count")
        assert resp.status_code == 200
        data = resp.json()
        assert "entries" in data
        assert len(data["entries"]) == 3

    def test_scan_entry_has_fields(self, client):
        resp = client.get("/operators/word_count/count")
        entries = resp.json()["entries"]
        for entry in entries:
            assert "key" in entry
            assert "value" in entry
            assert "timestamp_ms" in entry

    def test_scan_values_match(self, client):
        resp = client.get("/operators/word_count/count")
        entries = {e["key"]: e["value"] for e in resp.json()["entries"]}
        assert entries["hello"] == len("hello")
        assert entries["world"] == len("world")
        assert entries["foo"] == len("foo")

    def test_scan_limit(self, client):
        resp = client.get("/operators/word_count/count?limit=1")
        data = resp.json()
        assert len(data["entries"]) == 1

    def test_scan_pagination(self, client):
        """Paginate through all keys using next_cursor."""
        all_keys = []
        cursor = None
        while True:
            url = "/operators/word_count/count?limit=1"
            if cursor:
                url += f"&cursor={cursor}"
            resp = client.get(url)
            data = resp.json()
            all_keys.extend(e["key"] for e in data["entries"])
            cursor = data.get("next_cursor")
            if not cursor:
                break
        assert sorted(all_keys) == sorted(["hello", "world", "foo"])


# ---------------------------------------------------------------------------
# /operators/{op_id}/{state_name}/{key}
# ---------------------------------------------------------------------------


class TestGetEntryEndpoint:
    def test_get_existing_key(self, client):
        resp = client.get('/operators/word_count/count/"hello"')
        assert resp.status_code == 200
        data = resp.json()
        assert data["value"] == len("hello")

    def test_get_missing_key_404(self, client):
        resp = client.get('/operators/word_count/count/"not_there"')
        assert resp.status_code == 404

    def test_get_key_has_timestamp(self, client):
        resp = client.get('/operators/word_count/count/"world"')
        assert "timestamp_ms" in resp.json()

    def test_get_integer_key(self, client):
        """Integer keys encoded as JSON should work."""
        mgr = StateBackendManager(backend="memory")
        mgr.start()
        from ssb.topology.descriptor import OperatorDescriptor, TopologyDescriptor

        topo = TopologyDescriptor(
            version=1,
            operators={
                "op": OperatorDescriptor("op", ["st"]),
            },
        )
        mgr.set_topology(topo)
        mgr.get_state_context("op", 42).get_value_state("st").set("forty-two")
        app = create_app(mgr)
        c = TestClient(app)
        resp = c.get("/operators/op/st/42")
        assert resp.status_code == 200
        assert resp.json()["value"] == "forty-two"
        mgr.stop()
