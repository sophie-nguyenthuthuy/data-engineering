"""
Distributed Saga Orchestrator
==============================
Implements the Saga pattern for long-running, multi-step data workflows.
Each step declares a compensating transaction; on failure the orchestrator
automatically rolls back completed steps in reverse order.
"""

from .exceptions import (
    SagaAlreadyRunningError,
    SagaError,
    SagaNotFoundError,
    SagaNotRecoverableError,
    StepCompensationError,
    StepExecutionError,
)
from .orchestrator import SagaOrchestrator, SagaResult
from .persistence import SagaRecord, SagaStatus, SagaStore
from .step import RetryPolicy, SagaStep, StepRecord, StepResult, StepStatus

__all__ = [
    # Orchestrator
    "SagaOrchestrator",
    "SagaResult",
    # Steps
    "SagaStep",
    "StepRecord",
    "StepResult",
    "StepStatus",
    "RetryPolicy",
    # Persistence
    "SagaStore",
    "SagaRecord",
    "SagaStatus",
    # Exceptions
    "SagaError",
    "StepExecutionError",
    "StepCompensationError",
    "SagaAlreadyRunningError",
    "SagaNotFoundError",
    "SagaNotRecoverableError",
]
