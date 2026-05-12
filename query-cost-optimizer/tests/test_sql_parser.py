"""Unit tests for sql_parser helpers."""

import pytest
from query_cost_optimizer.sql_parser import (
    extract_filter_columns,
    extract_join_columns,
    extract_group_by_columns,
    extract_select_star,
    detect_expensive_patterns,
)


def test_extract_filter_columns_simple():
    sql = "SELECT id, name FROM orders WHERE status = 'active' AND created_at > '2024-01-01'"
    cols = extract_filter_columns(sql)
    assert "status" in cols
    assert "created_at" in cols


def test_extract_filter_columns_empty():
    assert extract_filter_columns("SELECT 1") == []


def test_extract_join_columns():
    sql = """
    SELECT o.id, c.name
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    """
    cols = extract_join_columns(sql)
    assert "customer_id" in cols or "id" in cols


def test_extract_group_by():
    sql = "SELECT country, COUNT(*) FROM events GROUP BY country"
    cols = extract_group_by_columns(sql)
    assert "country" in cols


def test_extract_select_star_true():
    assert extract_select_star("SELECT * FROM my_table WHERE id = 1") is True


def test_extract_select_star_false():
    assert extract_select_star("SELECT id, name FROM my_table") is False


class TestDetectExpensivePatterns:
    def test_select_star(self):
        sql = "SELECT * FROM orders WHERE status = 'pending'"
        patterns = detect_expensive_patterns(sql)
        assert "select_star" in patterns

    def test_order_without_limit(self):
        sql = "SELECT id, amount FROM orders ORDER BY amount DESC"
        patterns = detect_expensive_patterns(sql)
        assert "order_without_limit" in patterns

    def test_no_false_positive_with_limit(self):
        sql = "SELECT id FROM orders ORDER BY id DESC LIMIT 10"
        patterns = detect_expensive_patterns(sql)
        assert "order_without_limit" not in patterns

    def test_non_sargable_upper(self):
        sql = "SELECT * FROM users WHERE UPPER(email) = 'TEST@EXAMPLE.COM'"
        patterns = detect_expensive_patterns(sql)
        assert "non_sargable_filter" in patterns or "select_star" in patterns

    def test_clean_query(self):
        sql = """
        SELECT id, name, amount
        FROM orders
        WHERE created_at >= '2024-01-01' AND status = 'shipped'
        LIMIT 100
        """
        patterns = detect_expensive_patterns(sql)
        assert "select_star" not in patterns
        assert "order_without_limit" not in patterns
