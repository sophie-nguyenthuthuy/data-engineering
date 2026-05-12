"""
Financial Transfer Pipeline — 10-step Saga example
====================================================
Simulates a cross-currency, cross-ledger funds transfer with:

  Step 1  – ValidateTransferRequest      compensate: mark transfer voided
  Step 2  – ReserveSourceFunds           compensate: release reservation
  Step 3  – FetchExchangeRate            compensate: (no-op, read-only)
  Step 4  – CalculateFeesAndTax          compensate: (no-op, read-only)
  Step 5  – CreateAuditTrailEntry        compensate: mark audit entry cancelled
  Step 6  – DebitSourceAccount           compensate: credit back source
  Step 7  – CreditDestinationAccount     compensate: debit back destination
  Step 8  – SettleWithClearingHouse      compensate: submit reversal
  Step 9  – UpdateLedgerBalances         compensate: revert ledger entries
  Step 10 – SendNotifications            compensate: send cancellation notices

Run it:
    python -m examples.financial_pipeline
    python -m examples.financial_pipeline --fail-at 6   # trigger rollback at step 6
"""

from __future__ import annotations

import asyncio
import logging
import random
import sys
from typing import Any

from saga import RetryPolicy, SagaOrchestrator, SagaStep, SagaStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("financial_pipeline")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log(step: str, msg: str) -> None:
    logger.info("    [%s] %s", step, msg)


# ---------------------------------------------------------------------------
# Step 1 – Validate Transfer Request
# ---------------------------------------------------------------------------
class ValidateTransferRequest(SagaStep):
    retry_policy = RetryPolicy(max_attempts=3, backoff_base_seconds=0.1)

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        _log(self.name, f"Validating transfer {ctx['transfer_id']!r}: "
                        f"{ctx['source_account']} → {ctx['dest_account']} "
                        f"{ctx['amount']} {ctx['currency']}")
        if ctx["amount"] <= 0:
            raise ValueError("Transfer amount must be positive")
        if ctx["source_account"] == ctx["dest_account"]:
            raise ValueError("Source and destination accounts must differ")
        return {"validation_passed": True, "transfer_status": "validating"}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        _log(self.name, f"Voiding transfer {ctx['transfer_id']!r}")
        # e.g. UPDATE transfers SET status='voided' WHERE id=...
        ctx["transfer_status"] = "voided"


# ---------------------------------------------------------------------------
# Step 2 – Reserve Source Funds
# ---------------------------------------------------------------------------
class ReserveSourceFunds(SagaStep):
    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        _log(self.name, f"Placing hold of {ctx['amount']} {ctx['currency']} "
                        f"on {ctx['source_account']}")
        reservation_id = f"RSV-{ctx['transfer_id'][:8].upper()}"
        return {"reservation_id": reservation_id, "funds_reserved": True}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        rid = ctx.get("reservation_id", "unknown")
        _log(self.name, f"Releasing hold {rid} on {ctx['source_account']}")
        ctx["funds_reserved"] = False


# ---------------------------------------------------------------------------
# Step 3 – Fetch Exchange Rate (read-only, no compensation needed)
# ---------------------------------------------------------------------------
class FetchExchangeRate(SagaStep):
    retry_policy = RetryPolicy(max_attempts=5, backoff_base_seconds=0.2)

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        src, dst = ctx["currency"], ctx.get("dest_currency", ctx["currency"])
        rate = 1.0 if src == dst else round(random.uniform(0.85, 1.15), 6)
        _log(self.name, f"Exchange rate {src}→{dst}: {rate}")
        return {"exchange_rate": rate, "rate_source": "FX-Provider-v2"}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        _log(self.name, "No-op — rate fetch is read-only")


# ---------------------------------------------------------------------------
# Step 4 – Calculate Fees and Tax (read-only)
# ---------------------------------------------------------------------------
class CalculateFeesAndTax(SagaStep):
    FEE_RATE = 0.005   # 0.5 %
    TAX_RATE = 0.001   # 0.1 %

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        amount = ctx["amount"]
        fee = round(amount * self.FEE_RATE, 4)
        tax = round(amount * self.TAX_RATE, 4)
        net = round((amount - fee - tax) * ctx["exchange_rate"], 4)
        _log(self.name, f"amount={amount}  fee={fee}  tax={tax}  net_dest={net}")
        return {"fee": fee, "tax": tax, "net_dest_amount": net}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        _log(self.name, "No-op — fee calculation is read-only")


# ---------------------------------------------------------------------------
# Step 5 – Create Audit Trail Entry
# ---------------------------------------------------------------------------
class CreateAuditTrailEntry(SagaStep):
    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        audit_id = f"AUD-{ctx['transfer_id'][:8].upper()}"
        _log(self.name, f"Writing audit entry {audit_id}")
        return {"audit_id": audit_id}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        aid = ctx.get("audit_id", "unknown")
        _log(self.name, f"Marking audit entry {aid} as CANCELLED")


# ---------------------------------------------------------------------------
# Step 6 – Debit Source Account
# ---------------------------------------------------------------------------
class DebitSourceAccount(SagaStep):
    def __init__(self, fail: bool = False) -> None:
        self._fail = fail

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        total_debit = round(ctx["amount"] + ctx["fee"] + ctx["tax"], 4)
        if self._fail:
            raise RuntimeError(
                f"Core banking timeout: could not debit {ctx['source_account']}"
            )
        _log(self.name, f"Debiting {total_debit} {ctx['currency']} "
                        f"from {ctx['source_account']}")
        debit_txn = f"DBT-{ctx['transfer_id'][:8].upper()}"
        return {"debit_txn_id": debit_txn, "source_debited": True}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        txn = ctx.get("debit_txn_id", "unknown")
        total_debit = round(ctx["amount"] + ctx.get("fee", 0) + ctx.get("tax", 0), 4)
        _log(self.name, f"Reversing debit {txn}: crediting {total_debit} "
                        f"back to {ctx['source_account']}")


# ---------------------------------------------------------------------------
# Step 7 – Credit Destination Account
# ---------------------------------------------------------------------------
class CreditDestinationAccount(SagaStep):
    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        net = ctx["net_dest_amount"]
        dest_cur = ctx.get("dest_currency", ctx["currency"])
        _log(self.name, f"Crediting {net} {dest_cur} to {ctx['dest_account']}")
        credit_txn = f"CRD-{ctx['transfer_id'][:8].upper()}"
        return {"credit_txn_id": credit_txn, "dest_credited": True}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        txn = ctx.get("credit_txn_id", "unknown")
        _log(self.name, f"Reversing credit {txn}: debiting back from {ctx['dest_account']}")


# ---------------------------------------------------------------------------
# Step 8 – Settle with Clearing House
# ---------------------------------------------------------------------------
class SettleWithClearingHouse(SagaStep):
    retry_policy = RetryPolicy(max_attempts=3, backoff_base_seconds=0.5)

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        settlement_ref = f"CLR-{ctx['transfer_id'][:8].upper()}"
        _log(self.name, f"Submitting settlement {settlement_ref} to clearing house")
        return {"settlement_ref": settlement_ref, "settlement_status": "submitted"}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        ref = ctx.get("settlement_ref", "unknown")
        _log(self.name, f"Submitting reversal for settlement {ref}")


# ---------------------------------------------------------------------------
# Step 9 – Update Ledger Balances
# ---------------------------------------------------------------------------
class UpdateLedgerBalances(SagaStep):
    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        _log(self.name, "Posting double-entry ledger records")
        ledger_batch = f"LDG-{ctx['transfer_id'][:8].upper()}"
        return {"ledger_batch_id": ledger_batch, "ledger_updated": True}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        batch = ctx.get("ledger_batch_id", "unknown")
        _log(self.name, f"Reverting ledger batch {batch}")


# ---------------------------------------------------------------------------
# Step 10 – Send Notifications
# ---------------------------------------------------------------------------
class SendNotifications(SagaStep):
    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        _log(self.name, f"Sending success notifications for {ctx['transfer_id']!r}")
        return {"notifications_sent": True}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        _log(self.name, f"Sending cancellation notices for {ctx['transfer_id']!r}")


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def build_steps(fail_at: int | None) -> list[SagaStep]:
    """Build ordered step list; inject failure at *fail_at* (1-based)."""
    return [
        ValidateTransferRequest(),                        # 1
        ReserveSourceFunds(),                             # 2
        FetchExchangeRate(),                              # 3
        CalculateFeesAndTax(),                            # 4
        CreateAuditTrailEntry(),                          # 5
        DebitSourceAccount(fail=fail_at == 6),            # 6
        CreditDestinationAccount(),                       # 7
        SettleWithClearingHouse(),                        # 8
        UpdateLedgerBalances(),                           # 9
        SendNotifications(),                              # 10
    ]


async def run_transfer(
    transfer_id: str,
    source: str,
    dest: str,
    amount: float,
    currency: str = "USD",
    dest_currency: str = "EUR",
    fail_at: int | None = None,
    db_path: str = "sagas.db",
) -> None:
    store = SagaStore(db_path)
    orchestrator = SagaOrchestrator(store)

    initial_context = {
        "transfer_id": transfer_id,
        "source_account": source,
        "dest_account": dest,
        "amount": amount,
        "currency": currency,
        "dest_currency": dest_currency,
    }

    logger.info("=" * 60)
    logger.info("Starting financial transfer saga")
    logger.info("  Transfer : %s", transfer_id)
    logger.info("  From     : %s", source)
    logger.info("  To       : %s", dest)
    logger.info("  Amount   : %s %s", amount, currency)
    if fail_at:
        logger.info("  [TEST]   : Forcing failure at step %d", fail_at)
    logger.info("=" * 60)

    result = await orchestrator.run(
        steps=build_steps(fail_at),
        initial_context=initial_context,
        saga_type="financial_transfer",
        saga_id=transfer_id,
    )

    logger.info("=" * 60)
    if result.succeeded:
        logger.info("✅  Transfer %s COMPLETED", transfer_id)
        logger.info("    Net deposited : %s %s",
                    result.context.get("net_dest_amount"), dest_currency)
        logger.info("    Settlement    : %s", result.context.get("settlement_ref"))
        logger.info("    Ledger batch  : %s", result.context.get("ledger_batch_id"))
    else:
        logger.warning("❌  Transfer %s FAILED (status=%s)", transfer_id, result.status.value)
        logger.warning("    Failed at step : %s", result.failure_step)
        logger.warning("    Reason         : %s", result.failure_reason)
        if result.compensation_errors:
            logger.error("    Compensation errors:")
            for ce in result.compensation_errors:
                logger.error("      • %s: %s", ce["step"], ce["error"])
        else:
            logger.info("    ↩ All completed steps were rolled back successfully")
    logger.info("=" * 60)

    # Print step-by-step table
    print("\nStep summary:")
    print(f"  {'#':<4} {'Step':<35} {'Status':<22} {'Duration'}")
    print("  " + "-" * 70)
    for i, rec in enumerate(result.step_records, 1):
        dur = ""
        if rec.started_at and rec.completed_at:
            dur = f"{(rec.completed_at - rec.started_at) * 1000:.1f} ms"
        print(f"  {i:<4} {rec.name:<35} {rec.status.value:<22} {dur}")

    store.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Financial Transfer Saga demo")
    parser.add_argument("--fail-at", type=int, default=None,
                        help="Force failure at step N (1–10) to demo rollback")
    parser.add_argument("--amount", type=float, default=5000.0)
    args = parser.parse_args()

    asyncio.run(
        run_transfer(
            transfer_id="TXN-20260504-DEMO",
            source="ACC-001-USD",
            dest="ACC-002-EUR",
            amount=args.amount,
            fail_at=args.fail_at,
        )
    )
