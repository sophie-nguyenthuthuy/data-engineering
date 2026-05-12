"""Code generators: PhysicalPlan -> engine-specific runnable artefact.

Each codegen module exposes `emit(plan) -> str` (or `dict` for Dagster).
"""

from __future__ import annotations

from ppc.codegen.dagster import emit_dagster
from ppc.codegen.dbt_codegen import emit_dbt
from ppc.codegen.duckdb_codegen import emit_duckdb
from ppc.codegen.flink_codegen import emit_flink
from ppc.codegen.spark_codegen import emit_spark
from ppc.codegen.sql_codegen import emit_sql

__all__ = [
    "emit_dagster",
    "emit_dbt",
    "emit_duckdb",
    "emit_flink",
    "emit_spark",
    "emit_sql",
]
