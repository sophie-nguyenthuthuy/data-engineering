"""SQL extractor tests."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from psa.parser import parse_query


def test_parser_rejects_empty():
    with pytest.raises(ValueError):
        parse_query("   ")


def test_extracts_simple_where_equality():
    p = parse_query("SELECT * FROM orders WHERE country = 'US'")
    assert p.filter_columns == ("country",)
    assert p.join_columns == ()


def test_extracts_in_predicate():
    p = parse_query("SELECT * FROM o WHERE status IN ('a', 'b', 'c')")
    assert p.filter_columns == ("status",)


def test_extracts_between_predicate():
    p = parse_query("SELECT * FROM o WHERE event_ts BETWEEN '2024-01-01' AND '2024-12-31'")
    assert p.filter_columns == ("event_ts",)


def test_extracts_is_null():
    p = parse_query("SELECT * FROM o WHERE deleted_at IS NULL")
    assert p.filter_columns == ("deleted_at",)


def test_extracts_multiple_filters_and_dedupes():
    p = parse_query("SELECT * FROM o WHERE country = 'US' AND country = 'CA'")
    assert p.filter_columns == ("country",)


def test_extracts_join_keys():
    p = parse_query("SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id")
    assert "customer_id" in p.join_columns
    assert "id" in p.join_columns


def test_extracts_group_by_columns():
    p = parse_query("SELECT country, COUNT(*) FROM o GROUP BY country, region")
    assert set(p.group_columns) == {"country", "region"}


def test_strips_table_qualifier():
    p = parse_query("SELECT * FROM orders o WHERE o.customer_id = 1")
    assert p.filter_columns == ("customer_id",)


def test_ignores_order_by_columns():
    p = parse_query("SELECT * FROM o WHERE country = 'US' GROUP BY country ORDER BY total DESC")
    assert "country" in p.filter_columns
    assert "country" in p.group_columns
    assert "total" not in p.filter_columns


def test_combination_query():
    p = parse_query(
        "SELECT c.region, SUM(o.amount) FROM orders o "
        "JOIN customers c ON o.customer_id = c.customer_id "
        "WHERE o.event_ts >= '2024-01-01' AND c.region = 'EU' "
        "GROUP BY c.region"
    )
    assert set(p.filter_columns) == {"event_ts", "region"}
    assert "customer_id" in p.join_columns
    assert p.group_columns == ("region",)


@settings(max_examples=20, deadline=None)
@given(st.text(alphabet="abcdefghij", min_size=2, max_size=8))
def test_property_parser_is_deterministic(col):
    sql = f"SELECT * FROM t WHERE {col} = 1"
    assert parse_query(sql) == parse_query(sql)
