from abc import ABC, abstractmethod
from typing import Any


class BaseSource(ABC):
    def __init__(self, params: dict[str, Any]) -> None:
        self.params = params

    @abstractmethod
    def fetch(self, query: str, query_params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute query and return list of row dicts."""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(params={list(self.params.keys())})"
