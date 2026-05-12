"""
Seed demo data that showcases the composition accounting.

Creates:
  - 2 datasets: healthcare (sensitive) and banking
  - 3 analysts with different roles
  - Budget allocations with varying ε limits
  - 15 demo queries that demonstrate RDP/zCDP savings vs basic composition
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from app.database import SessionLocal, engine, Base
from app import models, budget_manager, schemas
from app.mechanisms import Mechanism, QueryType
from app.query_planner import execute_query

Base.metadata.create_all(bind=engine)
db = SessionLocal()


def seed():
    print("Seeding Privacy Budget Ledger demo data...")

    # ── Datasets ─────────────────────────────────────────────────────────────
    ds_health = budget_manager.create_dataset(db, schemas.DatasetCreate(
        name="patient_records",
        description="De-identified hospital patient records (age, diagnosis, treatment cost)",
        owner_id="owner-hospital",
        sensitivity=1.0,
        data_range_min=0.0,
        data_range_max=100000.0,
    ))

    ds_bank = budget_manager.create_dataset(db, schemas.DatasetCreate(
        name="transaction_logs",
        description="Banking transaction logs (amounts, merchant category)",
        owner_id="owner-bank",
        sensitivity=1.0,
        data_range_min=0.0,
        data_range_max=50000.0,
    ))

    print(f"  Created datasets: {ds_health.name}, {ds_bank.name}")

    # ── Analysts ──────────────────────────────────────────────────────────────
    a1 = budget_manager.create_analyst(db, schemas.AnalystCreate(
        username="alice_researcher",
        email="alice@research.org",
        role="senior_analyst",
    ))
    a2 = budget_manager.create_analyst(db, schemas.AnalystCreate(
        username="bob_ds",
        email="bob@datalab.io",
        role="analyst",
    ))
    a3 = budget_manager.create_analyst(db, schemas.AnalystCreate(
        username="carol_audit",
        email="carol@compliance.gov",
        role="auditor",
    ))

    print(f"  Created analysts: {a1.username}, {a2.username}, {a3.username}")

    # ── Budget allocations ────────────────────────────────────────────────────
    # Alice: large budget on health data, Gaussian mechanism
    alloc_a1_health = budget_manager.create_allocation(db, schemas.BudgetAllocationCreate(
        dataset_id=ds_health.id,
        analyst_id=a1.id,
        total_epsilon=5.0,
        total_delta=1e-5,
        exhaustion_policy="block",
        default_mechanism=Mechanism.GAUSSIAN,
    ))

    # Bob: tighter budget on health data
    alloc_b1_health = budget_manager.create_allocation(db, schemas.BudgetAllocationCreate(
        dataset_id=ds_health.id,
        analyst_id=a2.id,
        total_epsilon=2.0,
        total_delta=1e-5,
        exhaustion_policy="block",
        default_mechanism=Mechanism.GAUSSIAN,
    ))

    # Alice: banking data with Laplace
    alloc_a1_bank = budget_manager.create_allocation(db, schemas.BudgetAllocationCreate(
        dataset_id=ds_bank.id,
        analyst_id=a1.id,
        total_epsilon=3.0,
        total_delta=1e-5,
        exhaustion_policy="block",
        default_mechanism=Mechanism.LAPLACE,
    ))

    # Carol: auditor, small budget
    alloc_c1_health = budget_manager.create_allocation(db, schemas.BudgetAllocationCreate(
        dataset_id=ds_health.id,
        analyst_id=a3.id,
        total_epsilon=1.0,
        total_delta=1e-5,
        exhaustion_policy="block",
        default_mechanism=Mechanism.GAUSSIAN,
    ))

    print("  Created budget allocations")

    # ── Demo queries ──────────────────────────────────────────────────────────
    # Alice runs 10 Gaussian queries on health data (ε=0.3 each)
    # Under basic composition: 10 × 0.3 = 3.0 (hits limit if budget=5)
    # Under RDP/zCDP:  projected ε is much lower → more queries fit
    print("  Executing demo queries for Alice on patient_records...")
    for i in range(10):
        resp = execute_query(db, schemas.QueryRequest(
            dataset_id=ds_health.id,
            analyst_id=a1.id,
            query_type=QueryType.COUNT,
            query_text=f"Count of patients in age group {30 + i*5}-{35 + i*5}",
            true_result=float(1000 + i * 137),
            epsilon_requested=0.3,
            delta_requested=1e-5,
            sensitivity=1.0,
            mechanism=Mechanism.GAUSSIAN,
        ))
        print(
            f"    Query {i+1}: decision={resp.planner_decision}, "
            f"ε_rdp={resp.budget_remaining_rdp:.3f} remaining, "
            f"savings_vs_basic={resp.budget_savings_vs_basic:.3f}"
        )

    # Bob runs 5 queries to nearly exhaust his budget
    print("  Executing demo queries for Bob on patient_records...")
    for i in range(5):
        resp = execute_query(db, schemas.QueryRequest(
            dataset_id=ds_health.id,
            analyst_id=a2.id,
            query_type=QueryType.MEAN,
            query_text=f"Mean treatment cost for diagnosis group {i}",
            true_result=float(12000 + i * 500),
            epsilon_requested=0.35,
            delta_requested=1e-5,
            sensitivity=1.0,
            mechanism=Mechanism.GAUSSIAN,
        ))
        print(
            f"    Query {i+1}: decision={resp.planner_decision}"
        )

    # Alice runs Laplace queries on banking data
    print("  Executing Laplace queries for Alice on transaction_logs...")
    for i in range(3):
        resp = execute_query(db, schemas.QueryRequest(
            dataset_id=ds_bank.id,
            analyst_id=a1.id,
            query_type=QueryType.SUM,
            query_text=f"Total transaction volume for merchant category {i}",
            true_result=float(50000 + i * 7500),
            epsilon_requested=0.4,
            delta_requested=0.0,
            sensitivity=1.0,
            mechanism=Mechanism.LAPLACE,
        ))
        print(f"    Query {i+1}: decision={resp.planner_decision}")

    print("\nSeed complete. Run: uvicorn app.main:app --reload")
    db.close()


if __name__ == "__main__":
    seed()
