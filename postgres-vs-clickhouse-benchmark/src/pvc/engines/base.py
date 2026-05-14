"""Engine protocol.

Every engine implements the same three-method contract: ``setup`` once
to load DDL + sample data, ``execute(sql)`` to run one query and
return its rows, ``close`` to release resources. The benchmark runner
treats every engine the same way regardless of whether it's an
in-process SQLite, a remote Postgres, or a fake.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class EngineError(RuntimeError):
    """Raised when an engine fails to set up or execute a query."""


class Engine(ABC):
    """Pluggable query-engine interface."""

    name: str = "abstract"

    @abstractmethod
    def setup(self, ddl: list[str], inserts: list[tuple[str, list[tuple[Any, ...]]]]) -> None:
        """Apply ``ddl`` statements and bulk-load each ``(sql, rows)`` pair."""

    @abstractmethod
    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        """Run ``sql`` and return its result rows."""

    @abstractmethod
    def close(self) -> None:
        """Release any held resources."""


__all__ = ["Engine", "EngineError"]
