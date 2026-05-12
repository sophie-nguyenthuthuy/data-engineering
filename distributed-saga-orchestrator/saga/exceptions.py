from __future__ import annotations


class SagaError(Exception):
    pass


class StepExecutionError(SagaError):
    def __init__(self, step_name: str, cause: Exception) -> None:
        self.step_name = step_name
        self.cause = cause
        super().__init__(f"Step '{step_name}' failed: {cause}")


class StepCompensationError(SagaError):
    def __init__(self, step_name: str, cause: Exception) -> None:
        self.step_name = step_name
        self.cause = cause
        super().__init__(f"Compensation for '{step_name}' failed: {cause}")


class SagaAlreadyRunningError(SagaError):
    def __init__(self, saga_id: str) -> None:
        super().__init__(f"Saga '{saga_id}' is already running")


class SagaNotFoundError(SagaError):
    def __init__(self, saga_id: str) -> None:
        super().__init__(f"Saga '{saga_id}' not found in store")


class SagaNotRecoverableError(SagaError):
    """Raised when a saga cannot be recovered (e.g., already completed or failed)."""

    def __init__(self, saga_id: str, status: str) -> None:
        super().__init__(f"Saga '{saga_id}' cannot be recovered from status '{status}'")
