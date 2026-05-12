"""Abstract connector interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import pandas as pd
import sqlglot.expressions as exp


@dataclass
class ConnectorResult:
    data: pd.DataFrame
    rows_scanned: int
    rows_returned: int
    source: str
    table: str


class BaseConnector(ABC):
    """Every data source implements this interface."""

    @abstractmethod
    def fetch(
        self,
        table: str,
        columns: list[str],
        predicates: list[exp.Expression],
        limit: int | None = None,
        connection_params: dict[str, Any] | None = None,
    ) -> ConnectorResult:
        """Fetch rows matching the pushed-down predicates."""
        ...

    @abstractmethod
    def estimate_rows(
        self,
        table: str,
        predicates: list[exp.Expression],
        connection_params: dict[str, Any] | None = None,
    ) -> int:
        """Return a rough row-count estimate without fetching data."""
        ...

    # ------------------------------------------------------------------ #
    # Shared predicate translation helpers                                 #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _literal_value(expr: exp.Expression) -> Any:
        if isinstance(expr, exp.Literal):
            return expr.to_py()
        if isinstance(expr, exp.Boolean):
            return expr.this
        return str(expr)

    @staticmethod
    def _col_name(expr: exp.Column) -> str:
        return expr.name
