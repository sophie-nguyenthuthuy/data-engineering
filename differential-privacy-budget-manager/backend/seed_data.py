"""
Seed the database with demo datasets, analysts, and budget allocations.
Run: python seed_data.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app.database import SessionLocal, engine
from app import models
from app.privacy_mechanisms import Mechanism

models.Base.metadata.create_all(bind=engine)

db = SessionLocal()

# Datasets
datasets = [
    models.Dataset(id="ds-patients", name="Patient Records", description="Hospital patient data (age, diagnosis codes, treatment costs)", owner_id="owner-1", sensitivity=500.0, data_range_min=0, data_range_max=100000),
    models.Dataset(id="ds-transactions", name="Banking Transactions", description="Retail banking transactions 2020-2024", owner_id="owner-1", sensitivity=10000.0, data_range_min=-50000, data_range_max=50000),
    models.Dataset(id="ds-salaries", name="Employee Salaries", description="HR compensation data across departments", owner_id="owner-2", sensitivity=250000.0, data_range_min=30000, data_range_max=500000),
]

# Analysts
analysts = [
    models.Analyst(id="ana-alice", username="alice", email="alice@hospital.org", role="researcher"),
    models.Analyst(id="ana-bob", username="bob", email="bob@bank.com", role="analyst"),
    models.Analyst(id="ana-carol", username="carol", email="carol@hr.co", role="data_scientist"),
    models.Analyst(id="ana-dave", username="dave", email="dave@research.io", role="analyst"),
]

# Budget allocations
allocations = [
    models.BudgetAllocation(dataset_id="ds-patients", analyst_id="ana-alice", total_epsilon=5.0, exhaustion_policy="block", default_mechanism=Mechanism.LAPLACE),
    models.BudgetAllocation(dataset_id="ds-patients", analyst_id="ana-bob", total_epsilon=3.0, consumed_epsilon=2.8, exhaustion_policy="inject_noise", default_mechanism=Mechanism.GAUSSIAN, total_delta=1e-5),
    models.BudgetAllocation(dataset_id="ds-transactions", analyst_id="ana-bob", total_epsilon=10.0, consumed_epsilon=4.5, exhaustion_policy="block", default_mechanism=Mechanism.LAPLACE),
    models.BudgetAllocation(dataset_id="ds-transactions", analyst_id="ana-carol", total_epsilon=8.0, consumed_epsilon=8.0, exhaustion_policy="block", default_mechanism=Mechanism.LAPLACE),
    models.BudgetAllocation(dataset_id="ds-salaries", analyst_id="ana-carol", total_epsilon=6.0, consumed_epsilon=1.2, exhaustion_policy="block", default_mechanism=Mechanism.GAUSSIAN, total_delta=1e-5),
    models.BudgetAllocation(dataset_id="ds-salaries", analyst_id="ana-dave", total_epsilon=4.0, exhaustion_policy="inject_noise", default_mechanism=Mechanism.LAPLACE),
]

for obj in datasets + analysts + allocations:
    db.merge(obj)

db.commit()
db.close()
print("✓ Seed data loaded.")
