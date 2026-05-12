"""SQL frontend: SQL string -> logical plan via sqlglot."""

from __future__ import annotations

from ppc.frontend.sql import compile_sql, sql_to_logical

__all__ = ["compile_sql", "sql_to_logical"]
