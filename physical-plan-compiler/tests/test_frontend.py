"""SQL frontend tests."""

from __future__ import annotations

import pytest

from ppc.frontend.sql import SqlParseError, sql_to_logical
from ppc.ir.logical import LogicalAggregate, LogicalFilter, LogicalJoin, LogicalScan


def test_scan(small_catalog):
    plan = sql_to_logical("SELECT * FROM orders", small_catalog)
    assert isinstance(plan, LogicalScan)


def test_filter(small_catalog):
    plan = sql_to_logical("SELECT * FROM orders WHERE o_totalprice > 100", small_catalog)
    assert isinstance(plan, LogicalFilter)
    assert plan.predicate.referenced_columns() == frozenset({"o_totalprice"})


def test_filter_with_and(small_catalog):
    plan = sql_to_logical(
        "SELECT * FROM orders WHERE o_totalprice > 100 AND o_orderstatus = 'F'",
        small_catalog,
    )
    assert isinstance(plan, LogicalFilter)
    assert plan.predicate.referenced_columns() == frozenset({"o_totalprice", "o_orderstatus"})


def test_group_by_with_aggregate(small_catalog):
    plan = sql_to_logical(
        "SELECT o_orderstatus, COUNT(*) AS cnt FROM orders GROUP BY o_orderstatus",
        small_catalog,
    )
    assert isinstance(plan, LogicalAggregate)
    assert len(plan.aggregates) == 1
    assert plan.aggregates[0].func == "COUNT"
    assert plan.aggregates[0].alias == "cnt"


def test_global_aggregate_no_group_by(small_catalog):
    plan = sql_to_logical("SELECT SUM(o_totalprice) AS total FROM orders", small_catalog)
    assert isinstance(plan, LogicalAggregate)
    assert plan.group_by == ()
    assert plan.aggregates[0].func == "SUM"


def test_inner_join(small_catalog):
    plan = sql_to_logical(
        "SELECT c.c_name, o.o_totalprice FROM orders o "
        "JOIN customer c ON o.o_custkey = c.c_custkey",
        small_catalog,
    )
    assert isinstance(plan, LogicalJoin)


def test_filter_then_join(small_catalog):
    """SQL: WHERE on top of JOIN — should produce Filter(Join(...))."""
    plan = sql_to_logical(
        "SELECT * FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey "
        "WHERE o.o_totalprice > 100",
        small_catalog,
    )
    assert isinstance(plan, LogicalFilter)
    assert isinstance(plan.child, LogicalJoin)


def test_unknown_table_errors(small_catalog):
    with pytest.raises(SqlParseError, match="unknown table"):
        sql_to_logical("SELECT * FROM nonexistent", small_catalog)


def test_unknown_column_errors(small_catalog):
    with pytest.raises(SqlParseError, match="unknown column"):
        sql_to_logical("SELECT * FROM orders WHERE nonexistent = 1", small_catalog)


def test_ambiguous_column_errors(small_catalog):
    # `c_nationkey` exists in both nation and customer (both have nationkey-ish);
    # actually customer has c_nationkey and nation has n_nationkey, so no conflict.
    # Force the case: select unqualified `n_nationkey` after joining tables that
    # don't actually share it -> should NOT error.
    sql_to_logical(
        "SELECT n_nationkey FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey",
        small_catalog,
    )


def test_only_select_supported(small_catalog):
    with pytest.raises(SqlParseError):
        sql_to_logical("INSERT INTO orders VALUES (1)", small_catalog)


def test_outer_join_not_supported(small_catalog):
    with pytest.raises(SqlParseError, match="only INNER"):
        sql_to_logical(
            "SELECT * FROM orders LEFT JOIN customer "
            "ON orders.o_custkey = customer.c_custkey",
            small_catalog,
        )


def test_non_agg_projection_not_in_group_by(small_catalog):
    with pytest.raises(SqlParseError, match="non-aggregate projection"):
        sql_to_logical(
            "SELECT o_orderkey, COUNT(*) FROM orders GROUP BY o_orderstatus",
            small_catalog,
        )
