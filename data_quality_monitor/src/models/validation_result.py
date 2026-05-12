from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any
from pydantic import BaseModel, Field


class ValidationStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    ERROR = "error"


class ValidatorBackend(str, Enum):
    GREAT_EXPECTATIONS = "great_expectations"
    SODA = "soda"


class CheckResult(BaseModel):
    check_name: str
    expectation_type: str
    status: ValidationStatus
    observed_value: Any = None
    expected_value: Any = None
    element_count: int = 0
    unexpected_count: int = 0
    unexpected_percent: float = 0.0
    details: dict[str, Any] = Field(default_factory=dict)


class ValidationResult(BaseModel):
    result_id: str
    batch_id: str
    table_name: str
    backend: ValidatorBackend
    suite_name: str
    validated_at: datetime = Field(default_factory=datetime.utcnow)
    status: ValidationStatus
    pass_rate: float = Field(ge=0.0, le=1.0)
    total_checks: int
    passed_checks: int
    failed_checks: int
    warning_checks: int
    check_results: list[CheckResult] = Field(default_factory=list)
    row_count: int = 0
    duration_ms: float = 0.0
    error_message: str | None = None

    @property
    def blocks_downstream(self) -> bool:
        return self.status == ValidationStatus.FAILED
