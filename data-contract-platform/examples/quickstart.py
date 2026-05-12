"""
Quickstart example: validate a synthetic orders DataFrame against v1.0.0 contract.

Run:
    cd data-contract-platform
    pip install -e .
    python examples/quickstart.py
"""

from pathlib import Path

import pandas as pd

from dce.contract import load_contract
from dce.notifier import StdoutNotifier
from dce.reporter import consumer_notification
from dce.scorer import ReliabilityStore
from dce.validator import ContractValidator

CONTRACT_PATH = Path(__file__).parent.parent / "contracts" / "examples" / "orders" / "v1.0.0.yaml"
DB_PATH = Path("/tmp/dce_quickstart.db")

# ── Load the contract ───────────────────────────────────────────────
contract = load_contract(CONTRACT_PATH)
print(f"Loaded contract: {contract.id} @ {contract.version}")
print(f"Producer: {contract.producer}  |  Consumers: {', '.join(contract.consumers)}\n")

# ── Build a synthetic "pipeline output" ────────────────────────────
df = pd.DataFrame({
    "order_id":    [f"ORD-{i:04d}" for i in range(300)],
    "customer_id": [f"CUST-{i % 100:04d}" for i in range(300)],
    "order_date":  ["2026-05-09T08:00:00Z"] * 300,
    "status":      ["confirmed"] * 250 + ["shipped"] * 50,
    "total_amount": [round(i * 3.5 + 15, 2) for i in range(300)],
    "item_count":   [i % 8 + 1 for i in range(300)],
    "discount_pct": [None] * 300,
})

# Inject one bad row to trigger a semantic warning on status distribution
# Uncomment to see a failure:
# df.loc[0, "total_amount"] = -100.0

# ── Validate ────────────────────────────────────────────────────────
validator = ContractValidator(contract)
result = validator.validate(df, freshness_seconds=3600)

print("=" * 60)
print(f"Result: {'PASSED ✓' if result.passed else 'FAILED ✗'}")
print(f"Rows:   {result.stats['row_count']:,}")
print(f"Errors: {len(result.errors())}  Warnings: {len(result.warnings())}")

for issue in result.issues:
    tag = "[ERR]" if issue.severity == "error" else "[WRN]"
    print(f"  {tag} {issue.rule}: {issue.message}")
print()

# ── Store result & compute score ────────────────────────────────────
store = ReliabilityStore(DB_PATH)
store.record(result)

score = store.score(contract.producer, contract.id)
if score:
    print(f"Reliability score for '{contract.producer}': {score.reliability_score:.1%}"
          f"  ({score.passed_runs}/{score.total_runs} runs)")

# ── Notify consumers (stdout in this demo) ──────────────────────────
if not result.passed:
    payload = consumer_notification(result)
    notifier = StdoutNotifier()
    print("\n── Consumer Notification ──────────────────────────────────")
    notifier.send(payload)
