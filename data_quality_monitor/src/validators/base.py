from __future__ import annotations
import abc
from ..models import MicroBatch, ValidationResult


class BaseValidator(abc.ABC):
    """Contract every validator backend must implement."""

    @abc.abstractmethod
    async def validate(self, batch: MicroBatch) -> ValidationResult:
        """Run all checks for *batch* and return a consolidated result."""

    @abc.abstractmethod
    async def health_check(self) -> bool:
        """Return True if the validator backend is reachable and healthy."""
