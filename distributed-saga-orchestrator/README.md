# Distributed Saga Orchestrator

A production-grade implementation of the **Saga pattern** for long-running, multi-step data pipelines in Python.

Each step declares a **compensating transaction**. If step 6 of 10 fails, the orchestrator automatically rolls back steps 1–5 in **reverse order** — essential for financial-grade pipelines where partial writes are worse than no writes.

---

## Features

| Feature | Detail |
|---|---|
| **Automatic rollback** | Compensations run in strict reverse order on any failure |
| **SQLite-backed durability** | Every step transition is persisted; survives process crashes |
| **Crash recovery** | `orchestrator.recover()` resumes a stuck saga from last known state |
| **Per-step retry policy** | Configurable `max_attempts`, exponential back-off, retryable exception types |
| **Context propagation** | Each step's output is merged into a shared context available to all downstream steps |
| **Compensation fault-tolerance** | A failing compensation logs an error and continues rolling back remaining steps |
| **Zero runtime dependencies** | Standard library only (`asyncio`, `sqlite3`, `dataclasses`) |
| **Async-native** | Built on `asyncio`; steps are `async def` coroutines |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   SagaOrchestrator                       │
│                                                          │
│  run(steps, initial_context)                             │
│  ┌──────┐  ┌──────┐  ┌──────┐  ✗ ┌──────┐  ┌──────┐  │
│  │Step 1│→ │Step 2│→ │Step 3│→   │Step 4│  │Step 5│  │
│  └──────┘  └──────┘  └──────┘    └──────┘  └──────┘  │
│      ↑          ↑         ↑        FAILS               │
│   comp()     comp()    comp()   ← rolled back in        │
│  (step 3)  (step 2)  (step 1)     reverse order        │
│                                                          │
│  State persisted to SagaStore (SQLite) after each step  │
└─────────────────────────────────────────────────────────┘
```

### State machine

```
PENDING → RUNNING → COMPLETED
                 ↘
              COMPENSATING → COMPENSATED   (all compensations ok)
                           → FAILED        (≥1 compensation also failed)
```

---

## Quick Start

```bash
git clone https://github.com/<you>/distributed-saga-orchestrator
cd distributed-saga-orchestrator
pip install -e ".[dev]"
```

### Define steps

```python
from saga import SagaStep, RetryPolicy
from typing import Any

class DebitAccount(SagaStep):
    retry_policy = RetryPolicy(max_attempts=3, backoff_base_seconds=0.5)

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        # ... call your banking API ...
        return {"debit_txn_id": "DBT-001"}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        txn_id = ctx["debit_txn_id"]
        # ... reverse the debit ...

class CreditAccount(SagaStep):
    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        return {"credit_txn_id": "CRD-001"}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        # ... reverse the credit ...
```

### Run a saga

```python
import asyncio
from saga import SagaOrchestrator, SagaStore

async def main():
    store = SagaStore("sagas.db")           # use ":memory:" for testing
    orchestrator = SagaOrchestrator(store)

    result = await orchestrator.run(
        steps=[DebitAccount(), CreditAccount()],
        initial_context={"amount": 500.00, "currency": "USD"},
        saga_type="funds_transfer",
    )

    if result.succeeded:
        print("Transfer complete:", result.context)
    else:
        print("Transfer failed at:", result.failure_step)
        print("Reason:", result.failure_reason)
        print("Compensation errors:", result.compensation_errors)

asyncio.run(main())
```

### Recover a crashed saga

```python
# If your process died while a saga was RUNNING or COMPENSATING:
result = await orchestrator.recover(saga_id="TXN-20260504-DEMO", steps=build_steps())
```

---

## Examples

### Financial Transfer Pipeline (10 steps)

Simulates a cross-currency, cross-ledger funds transfer:

```
Step  1  ValidateTransferRequest    compensate: void transfer record
Step  2  ReserveSourceFunds         compensate: release hold
Step  3  FetchExchangeRate          compensate: no-op (read-only)
Step  4  CalculateFeesAndTax        compensate: no-op (read-only)
Step  5  CreateAuditTrailEntry      compensate: mark entry CANCELLED
Step  6  DebitSourceAccount         compensate: credit back source
Step  7  CreditDestinationAccount   compensate: debit back destination
Step  8  SettleWithClearingHouse    compensate: submit reversal
Step  9  UpdateLedgerBalances       compensate: revert ledger batch
Step 10  SendNotifications          compensate: send cancellation notices
```

```bash
# Happy path
python -m examples.financial_pipeline

# Force failure at step 6 to see rollback of steps 1–5
python -m examples.financial_pipeline --fail-at 6
```

### ETL Data Pipeline (8 steps)

```bash
python -m examples.etl_pipeline
python -m examples.etl_pipeline --fail-at 6   # rollback from production swap
```

---

## API Reference

### `SagaOrchestrator`

```python
class SagaOrchestrator:
    def __init__(self, store: SagaStore | None = None): ...

    async def run(
        self,
        steps: list[SagaStep],
        initial_context: dict | None = None,
        saga_type: str = "saga",
        saga_id: str | None = None,
    ) -> SagaResult: ...

    async def recover(self, saga_id: str, steps: list[SagaStep]) -> SagaResult: ...
```

### `SagaStep` (abstract base class)

```python
class SagaStep(ABC):
    retry_policy: RetryPolicy = RetryPolicy()   # override per step

    @property
    def name(self) -> str: ...          # defaults to class name

    async def execute(self, ctx: dict) -> dict: ...     # raise to trigger rollback
    async def compensate(self, ctx: dict) -> None: ...  # idempotent undo
```

### `RetryPolicy`

```python
@dataclass
class RetryPolicy:
    max_attempts: int = 1
    backoff_base_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 60.0
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,)
```

### `SagaResult`

```python
result.succeeded          # bool
result.status             # SagaStatus enum
result.saga_id            # str
result.context            # dict — final accumulated context
result.failure_step       # str | None
result.failure_reason     # str | None
result.compensation_errors  # list[dict]  — errors during rollback
result.step_records       # list[StepRecord]
```

### `SagaStore`

```python
store = SagaStore()                    # in-memory
store = SagaStore("path/to/sagas.db") # file-backed (durable)

store.save(record)
store.load(saga_id)                    # -> SagaRecord | None
store.list_by_status(SagaStatus.COMPENSATING)
store.list_by_type("financial_transfer")
```

---

## Running Tests

```bash
pip install -e ".[dev]"
pytest                         # all tests
pytest tests/test_rollback.py  # rollback / compensation tests only
pytest --cov=saga --cov-report=term-missing
```

---

## Project Structure

```
distributed-saga-orchestrator/
├── saga/
│   ├── __init__.py          # public API surface
│   ├── orchestrator.py      # SagaOrchestrator + SagaResult
│   ├── step.py              # SagaStep, StepRecord, RetryPolicy
│   ├── persistence.py       # SagaStore (SQLite), SagaRecord, SagaStatus
│   └── exceptions.py        # typed exception hierarchy
├── examples/
│   ├── financial_pipeline.py  # 10-step cross-currency transfer
│   └── etl_pipeline.py        # 8-step batch ETL workflow
├── tests/
│   ├── test_orchestrator.py   # happy path, context, retry, recovery
│   ├── test_rollback.py       # compensation ordering & fault handling
│   └── test_persistence.py    # SagaStore round-trips & edge cases
├── pyproject.toml
└── requirements-dev.txt
```

---

## License

MIT
