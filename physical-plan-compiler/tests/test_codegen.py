"""Code generators produce syntactically valid output."""

from __future__ import annotations

import ast

from ppc.cascades.optimizer import Optimizer
from ppc.codegen import emit_dagster, emit_dbt, emit_duckdb, emit_flink, emit_spark
from ppc.frontend.sql import sql_to_logical


def _plan(sql, catalog):
    return Optimizer(catalog=catalog).optimize(sql_to_logical(sql, catalog))


def test_duckdb_codegen_is_valid_python(small_catalog):
    code = emit_duckdb(_plan("SELECT * FROM orders WHERE o_totalprice > 100", small_catalog))
    # Must parse as Python
    ast.parse(code)
    assert "duckdb" in code
    assert "SELECT" in code.upper()


def test_spark_codegen_is_valid_python(small_catalog):
    code = emit_spark(_plan(
        "SELECT o_orderstatus, COUNT(*) AS cnt FROM orders GROUP BY o_orderstatus",
        small_catalog,
    ))
    ast.parse(code)
    assert "spark.read" in code
    assert "groupBy" in code or ".agg(" in code


def test_dbt_codegen_uses_jinja_config(small_catalog):
    code = emit_dbt(_plan(
        "SELECT o_orderstatus, COUNT(*) AS cnt FROM orders GROUP BY o_orderstatus",
        small_catalog,
    ))
    assert "{{ config(" in code
    assert "materialized" in code


def test_flink_codegen_emits_sql_insert(small_catalog):
    code = emit_flink(_plan(
        "SELECT SUM(o_totalprice) AS total FROM orders WHERE o_totalprice > 100",
        small_catalog,
    ))
    assert "INSERT INTO" in code
    assert "SELECT" in code


def test_dagster_manifest_structure(small_catalog):
    m = emit_dagster(_plan(
        "SELECT c_name, o_totalprice FROM orders o "
        "JOIN customer c ON o.o_custkey = c.c_custkey",
        small_catalog,
    ))
    assert m["version"] == "1.0"
    assert "assets" in m
    assert m["estimated_cost"] > 0
    # Asset IDs are unique
    ids = [a["asset_id"] for a in m["assets"]]
    assert len(set(ids)) == len(ids)


def test_codegen_handles_complex_predicate(small_catalog):
    """Compound predicates render correctly."""
    code = emit_duckdb(_plan(
        "SELECT * FROM orders WHERE o_totalprice > 100 AND o_orderstatus = 'F'",
        small_catalog,
    ))
    assert "AND" in code or "and" in code


def test_codegen_join(small_catalog):
    code = emit_spark(_plan(
        "SELECT c_name FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey",
        small_catalog,
    ))
    ast.parse(code)
    assert ".join(" in code
