import pytest
from catalog.lineage import extract_lineage, parse_column_refs


class TestParseColumnRefs:
    def test_three_part(self):
        s, t, c = parse_column_refs("myschema.mytable.mycolumn")
        assert s == "myschema"
        assert t == "mytable"
        assert c == "mycolumn"

    def test_two_part(self):
        s, t, c = parse_column_refs("mytable.mycolumn")
        assert s is None
        assert t == "mytable"
        assert c == "mycolumn"

    def test_one_part(self):
        s, t, c = parse_column_refs("mycolumn")
        assert s is None
        assert t is None
        assert c == "mycolumn"


class TestExtractLineage:
    def test_simple_insert_select(self):
        sql = """
        INSERT INTO stg_users (user_id, email, phone)
        SELECT user_id, email, phone FROM raw_users
        """
        edges = extract_lineage(sql)
        assert len(edges) > 0
        targets = [e["target"] for e in edges]
        assert any("email" in t for t in targets)
        assert any("user_id" in t for t in targets)

    def test_create_table_as_select(self):
        sql = """
        CREATE TABLE reporting AS
        SELECT u.user_id, u.email, SUM(o.total_amount) AS total_spent
        FROM users u
        JOIN orders o ON u.user_id = o.user_id
        GROUP BY u.user_id, u.email
        """
        edges = extract_lineage(sql)
        targets = [e["target"] for e in edges]
        assert any("total_spent" in t for t in targets)
        assert any("email" in t for t in targets)

    def test_aliased_columns(self):
        sql = """
        INSERT INTO output (full_name)
        SELECT first_name || ' ' || last_name AS full_name FROM input
        """
        edges = extract_lineage(sql)
        targets = [e["target"] for e in edges]
        assert any("full_name" in t for t in targets)

    def test_no_edges_for_bare_select(self):
        sql = "SELECT id, name FROM users"
        edges = extract_lineage(sql)
        assert edges == []

    def test_empty_sql(self):
        assert extract_lineage("") == []

    def test_invalid_sql_returns_empty(self):
        assert extract_lineage("NOT VALID SQL !!!") == []

    def test_transform_captured(self):
        sql = """
        INSERT INTO sales_enriched (total)
        SELECT amount * 1.1 AS total FROM raw_sales
        """
        edges = extract_lineage(sql)
        assert len(edges) > 0
        assert edges[0]["transform"] is not None
