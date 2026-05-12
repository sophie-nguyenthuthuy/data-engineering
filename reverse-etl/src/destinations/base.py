from abc import ABC, abstractmethod
from typing import Any


class BaseDestination(ABC):
    def __init__(self, params: dict[str, Any]) -> None:
        self.params = params

    @abstractmethod
    def send(self, records: list[dict[str, Any]]) -> int:
        """Push records to the destination. Returns count of records synced."""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(params={list(self.params.keys())})"
