"""
Integration tests for the query planner via the FastAPI test client.
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main import app
from app.database import Base, get_db
from app.mechanisms import Mechanism, QueryType


# ---------------------------------------------------------------------------
# In-memory SQLite for tests
# ---------------------------------------------------------------------------

TEST_DB_URL = "sqlite:///./test_planner.db"

engine = create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
TestingSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    db = TestingSession()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(autouse=True, scope="module")
def setup_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


@pytest.fixture(scope="module")
def seeded(client):
    """Create one dataset, one analyst, one allocation and return their IDs."""
    ds = client.post("/datasets/", json={
        "name": "test_ds", "owner_id": "owner1", "sensitivity": 1.0,
    }).json()
    a = client.post("/analysts/", json={
        "username": "tester", "email": "test@test.com",
    }).json()
    alloc = client.post("/budgets/", json={
        "dataset_id": ds["id"],
        "analyst_id": a["id"],
        "total_epsilon": 3.0,
        "total_delta": 1e-5,
        "default_mechanism": "gaussian",
    }).json()
    return {"dataset_id": ds["id"], "analyst_id": a["id"], "alloc": alloc}


class TestHealthEndpoints:
    def test_root(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert "accounting_modes" in r.json()

    def test_health(self, client):
        r = client.get("/health")
        assert r.status_code == 200


class TestPlanDryRun:
    def test_accept_small_query(self, client, seeded):
        r = client.post("/planner/plan", json={
            "dataset_id": seeded["dataset_id"],
            "analyst_id": seeded["analyst_id"],
            "query_type": "count",
            "epsilon_requested": 0.1,
            "delta_requested": 1e-5,
            "sensitivity": 1.0,
            "mechanism": "gaussian",
        })
        assert r.status_code == 200
        body = r.json()
        assert body["decision"] == "accept"
        assert body["projected_epsilon_rdp"] <= 3.0

    def test_reject_huge_query(self, client, seeded):
        r = client.post("/planner/plan", json={
            "dataset_id": seeded["dataset_id"],
            "analyst_id": seeded["analyst_id"],
            "query_type": "count",
            "epsilon_requested": 100.0,  # absurdly large
            "delta_requested": 1e-5,
            "sensitivity": 1.0,
            "mechanism": "gaussian",
        })
        assert r.status_code == 200
        body = r.json()
        assert body["decision"] in ("rewrite", "reject")

    def test_plan_does_not_debit_budget(self, client, seeded):
        # Get summary before
        s1 = client.get(
            f"/budgets/{seeded['dataset_id']}/{seeded['analyst_id']}/summary"
        ).json()
        # Run a plan (dry-run)
        client.post("/planner/plan", json={
            "dataset_id": seeded["dataset_id"],
            "analyst_id": seeded["analyst_id"],
            "query_type": "count",
            "epsilon_requested": 0.5,
            "delta_requested": 1e-5,
            "sensitivity": 1.0,
            "mechanism": "gaussian",
        })
        s2 = client.get(
            f"/budgets/{seeded['dataset_id']}/{seeded['analyst_id']}/summary"
        ).json()
        assert s1["consumed_epsilon_basic"] == s2["consumed_epsilon_basic"]


class TestExecuteQuery:
    def test_execute_accept(self, client, seeded):
        r = client.post("/planner/execute", json={
            "dataset_id": seeded["dataset_id"],
            "analyst_id": seeded["analyst_id"],
            "query_type": "count",
            "query_text": "count test",
            "true_result": 500.0,
            "epsilon_requested": 0.2,
            "delta_requested": 1e-5,
            "sensitivity": 1.0,
            "mechanism": "gaussian",
        })
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "allowed"
        assert body["result"] is not None
        assert body["planner_decision"] in ("accept", "rewrite")

    def test_budget_debited_after_execute(self, client, seeded):
        s_before = client.get(
            f"/budgets/{seeded['dataset_id']}/{seeded['analyst_id']}/summary"
        ).json()
        client.post("/planner/execute", json={
            "dataset_id": seeded["dataset_id"],
            "analyst_id": seeded["analyst_id"],
            "query_type": "count",
            "query_text": "post-test count",
            "true_result": 100.0,
            "epsilon_requested": 0.1,
            "delta_requested": 1e-5,
            "sensitivity": 1.0,
            "mechanism": "gaussian",
        })
        s_after = client.get(
            f"/budgets/{seeded['dataset_id']}/{seeded['analyst_id']}/summary"
        ).json()
        assert s_after["consumed_epsilon_basic"] > s_before["consumed_epsilon_basic"]
        assert s_after["consumed_rho"] > s_before["consumed_rho"]

    def test_rdp_tighter_than_basic(self, client, seeded):
        """After several queries, RDP ε < basic ε."""
        # Execute several queries
        for _ in range(5):
            client.post("/planner/execute", json={
                "dataset_id": seeded["dataset_id"],
                "analyst_id": seeded["analyst_id"],
                "query_type": "count",
                "query_text": "bulk count",
                "true_result": 200.0,
                "epsilon_requested": 0.15,
                "delta_requested": 1e-5,
                "sensitivity": 1.0,
                "mechanism": "gaussian",
            })
        summary = client.get(
            f"/budgets/{seeded['dataset_id']}/{seeded['analyst_id']}/summary"
        ).json()
        assert summary["consumed_epsilon_rdp"] < summary["consumed_epsilon_basic"]
        assert summary["savings_epsilon"] > 0

    def test_reject_returns_403(self, client, seeded):
        # Exhaust budget via direct DB manipulation (consumed_rho >> total_rho),
        # then verify the execute endpoint returns 403.
        ds = client.post("/datasets/", json={
            "name": "exhaust_ds", "owner_id": "owner3", "sensitivity": 1.0,
        }).json()
        a = client.post("/analysts/", json={
            "username": "exhaust_analyst", "email": "exhaust@test.com",
        }).json()
        alloc_resp = client.post("/budgets/", json={
            "dataset_id": ds["id"],
            "analyst_id": a["id"],
            "total_epsilon": 0.5,
            "total_delta": 1e-5,
            "default_mechanism": "gaussian",
        }).json()

        # Force both consumed_rho AND accumulated_rdp to exhausted state so that
        # even σ→∞ produces projected ε >> total_epsilon under both accountants.
        db = TestingSession()
        from app.models import BudgetAllocation
        from app.composition.rdp import ALPHA_ORDERS
        alloc = db.query(BudgetAllocation).filter_by(
            dataset_id=ds["id"], analyst_id=a["id"]
        ).first()
        alloc.consumed_rho = alloc.total_rho + 10.0
        alloc.accumulated_rdp_json = [[a, 1000.0] for a in ALPHA_ORDERS]
        db.commit()
        db.close()

        r = client.post("/planner/execute", json={
            "dataset_id": ds["id"],
            "analyst_id": a["id"],
            "query_type": "count",
            "query_text": "over-budget query",
            "true_result": 100.0,
            "epsilon_requested": 0.5,
            "delta_requested": 1e-5,
            "sensitivity": 1.0,
            "mechanism": "gaussian",
        })
        assert r.status_code == 403


class TestLedgerAudit:
    def test_ledger_entries_populated(self, client, seeded):
        r = client.get(
            f"/budgets/{seeded['dataset_id']}/{seeded['analyst_id']}/ledger"
        )
        assert r.status_code == 200
        entries = r.json()
        assert len(entries) > 0
        for entry in entries:
            assert "rho" in entry
            assert "epsilon_basic" in entry
            assert "projected_epsilon_rdp" in entry

    def test_savings_accumulate(self, client, seeded):
        entries = client.get(
            f"/budgets/{seeded['dataset_id']}/{seeded['analyst_id']}/ledger"
        ).json()
        last = entries[-1]
        if last["savings_vs_basic"] is not None:
            assert last["savings_vs_basic"] >= 0
