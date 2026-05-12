"""Tests for the SQL frontend (lexer, parser, planner)."""
import pytest
from adaptive_engine import Catalog, AdaptiveEngine
from adaptive_engine.sql import Parser, Planner, LexError, ParseError, PlanError
from adaptive_engine.sql.lexer import tokenize, TT
from adaptive_engine.plan import (
    AggregateNode, FilterNode, HashJoinNode, LimitNode,
    ProjectNode, ScanNode, SortNode,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

def make_catalog() -> Catalog:
    catalog = Catalog()
    catalog.create_table(
        "employees",
        [{"id": i, "name": f"Emp{i}", "dept_id": i % 5, "salary": 30_000 + i * 1_000, "active": i % 3 != 0}
         for i in range(50)],
    )
    catalog.create_table(
        "departments",
        [{"id": i, "name": f"Dept{i}", "budget": i * 10_000} for i in range(5)],
    )
    catalog.create_table(
        "projects",
        [{"id": i, "dept_id": i % 5, "cost": i * 500} for i in range(20)],
    )
    return catalog


# ------------------------------------------------------------------
# Lexer
# ------------------------------------------------------------------

class TestLexer:
    def test_keywords(self):
        tokens = tokenize("SELECT FROM WHERE JOIN ON")
        types = [t.type for t in tokens if t.type != TT.EOF]
        assert types == [TT.SELECT, TT.FROM, TT.WHERE, TT.JOIN, TT.ON]

    def test_integer_and_float(self):
        tokens = tokenize("42 3.14")
        assert tokens[0].type == TT.INTEGER and tokens[0].value == 42
        assert tokens[1].type == TT.FLOAT and abs(tokens[1].value - 3.14) < 1e-9

    def test_string_literal(self):
        tokens = tokenize("'hello world'")
        assert tokens[0].type == TT.STRING and tokens[0].value == "hello world"

    def test_operators(self):
        tokens = tokenize("= != <> < <= > >=")
        types = [t.type for t in tokens if t.type != TT.EOF]
        assert types == [TT.EQ, TT.NEQ, TT.NEQ, TT.LT, TT.LTE, TT.GT, TT.GTE]

    def test_identifiers(self):
        tokens = tokenize("my_table col1 _x")
        types = [t.type for t in tokens if t.type != TT.EOF]
        assert all(t == TT.IDENT for t in types)

    def test_lex_error(self):
        with pytest.raises(LexError):
            tokenize("SELECT @bad")

    def test_comment_skipped(self):
        tokens = tokenize("SELECT -- this is a comment\nFROM")
        types = [t.type for t in tokens if t.type != TT.EOF]
        assert types == [TT.SELECT, TT.FROM]


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------

class TestParser:
    def test_simple_select(self):
        q = Parser.parse("SELECT id, name FROM employees")
        assert q.from_table == "employees"
        assert len(q.select) == 2

    def test_select_star(self):
        q = Parser.parse("SELECT * FROM employees")
        assert len(q.select) == 1

    def test_where_clause(self):
        q = Parser.parse("SELECT id FROM employees WHERE salary > 50000")
        assert q.where is not None

    def test_and_or_condition(self):
        q = Parser.parse("SELECT id FROM employees WHERE salary > 40000 AND active = true")
        from adaptive_engine.sql.parser import SqlBinOp
        assert isinstance(q.where, SqlBinOp) and q.where.op == "AND"

    def test_join(self):
        q = Parser.parse(
            "SELECT e.id FROM employees e JOIN departments d ON e.dept_id = d.id"
        )
        assert len(q.joins) == 1
        assert q.joins[0].table == "departments"

    def test_group_by(self):
        q = Parser.parse("SELECT dept_id, COUNT(*) cnt FROM employees GROUP BY dept_id")
        assert len(q.group_by) == 1

    def test_order_by(self):
        q = Parser.parse("SELECT id FROM employees ORDER BY salary DESC")
        assert len(q.order_by) == 1
        assert q.order_by[0][1] is False  # descending

    def test_limit_offset(self):
        q = Parser.parse("SELECT id FROM employees LIMIT 10 OFFSET 5")
        assert q.limit == 10
        assert q.offset == 5

    def test_between(self):
        q = Parser.parse("SELECT id FROM employees WHERE salary BETWEEN 40000 AND 60000")
        from adaptive_engine.sql.parser import SqlBinOp
        assert isinstance(q.where, SqlBinOp) and q.where.op == "AND"

    def test_is_null(self):
        q = Parser.parse("SELECT id FROM employees WHERE name IS NULL")
        from adaptive_engine.sql.parser import SqlIsNull
        assert isinstance(q.where, SqlIsNull) and not q.where.negated

    def test_is_not_null(self):
        q = Parser.parse("SELECT id FROM employees WHERE name IS NOT NULL")
        from adaptive_engine.sql.parser import SqlIsNull
        assert isinstance(q.where, SqlIsNull) and q.where.negated

    def test_aggregate_functions(self):
        q = Parser.parse("SELECT COUNT(*), SUM(salary), AVG(salary) FROM employees")
        from adaptive_engine.sql.parser import SqlAgg
        aggs = [i.expr for i in q.select if isinstance(i.expr, SqlAgg)]
        funcs = {a.func for a in aggs}
        assert funcs == {"count", "sum", "avg"}

    def test_parse_error_missing_from(self):
        with pytest.raises(ParseError):
            Parser.parse("SELECT id employees")

    def test_distinct(self):
        q = Parser.parse("SELECT DISTINCT dept_id FROM employees")
        assert q.distinct is True


# ------------------------------------------------------------------
# Planner → execution
# ------------------------------------------------------------------

class TestPlanner:
    def test_simple_scan(self):
        catalog = make_catalog()
        planner = Planner(catalog)
        plan = planner.plan_sql("SELECT * FROM employees")
        assert isinstance(plan, ScanNode)

    def test_project(self):
        catalog = make_catalog()
        planner = Planner(catalog)
        plan = planner.plan_sql("SELECT id, name FROM employees")
        assert isinstance(plan, ProjectNode)
        assert "id" in plan.columns and "name" in plan.columns

    def test_filter(self):
        catalog = make_catalog()
        planner = Planner(catalog)
        plan = planner.plan_sql("SELECT * FROM employees WHERE salary > 40000")
        # Outermost is scan (no project for *), but filter should be in tree
        from adaptive_engine.plan import walk
        node_types = {type(n).__name__ for n in walk(plan)}
        assert "FilterNode" in node_types

    def test_join_plan(self):
        catalog = make_catalog()
        planner = Planner(catalog)
        plan = planner.plan_sql(
            "SELECT e.id, d.name FROM employees e "
            "JOIN departments d ON e.dept_id = d.id"
        )
        from adaptive_engine.plan import walk
        node_types = {type(n).__name__ for n in walk(plan)}
        assert "HashJoinNode" in node_types

    def test_aggregate_plan(self):
        catalog = make_catalog()
        planner = Planner(catalog)
        plan = planner.plan_sql(
            "SELECT dept_id, COUNT(*) AS cnt, SUM(salary) AS total "
            "FROM employees GROUP BY dept_id"
        )
        from adaptive_engine.plan import walk
        node_types = {type(n).__name__ for n in walk(plan)}
        assert "AggregateNode" in node_types

    def test_order_by_plan(self):
        catalog = make_catalog()
        planner = Planner(catalog)
        plan = planner.plan_sql("SELECT id FROM employees ORDER BY salary DESC")
        from adaptive_engine.plan import walk
        assert any(isinstance(n, SortNode) for n in walk(plan))

    def test_limit_plan(self):
        catalog = make_catalog()
        planner = Planner(catalog)
        plan = planner.plan_sql("SELECT id FROM employees LIMIT 5")
        from adaptive_engine.plan import walk
        assert any(isinstance(n, LimitNode) for n in walk(plan))


# ------------------------------------------------------------------
# End-to-end: SQL → execute → results
# ------------------------------------------------------------------

class TestSQLExecution:
    def _run(self, sql: str, catalog: Catalog | None = None) -> list[dict]:
        c = catalog or make_catalog()
        engine = AdaptiveEngine(c)
        plan = Planner(c).plan_sql(sql)
        rows, _ = engine.execute(plan)
        return rows

    def test_full_scan(self):
        rows = self._run("SELECT * FROM employees")
        assert len(rows) == 50

    def test_select_columns(self):
        rows = self._run("SELECT id, salary FROM employees")
        assert all(set(r.keys()) == {"id", "salary"} for r in rows)

    def test_filter_gt(self):
        rows = self._run("SELECT * FROM employees WHERE salary > 60000")
        assert all(r["salary"] > 60_000 for r in rows)
        assert len(rows) > 0

    def test_filter_eq(self):
        rows = self._run("SELECT * FROM employees WHERE dept_id = 0")
        assert all(r["dept_id"] == 0 for r in rows)

    def test_filter_and(self):
        rows = self._run(
            "SELECT * FROM employees WHERE salary > 40000 AND active = true"
        )
        assert all(r["salary"] > 40_000 for r in rows)

    def test_join_execution(self):
        rows = self._run(
            "SELECT e.id, d.name FROM employees e "
            "JOIN departments d ON e.dept_id = d.id"
        )
        assert len(rows) > 0
        assert all("name" in r for r in rows)

    def test_aggregate_count(self):
        rows = self._run(
            "SELECT dept_id, COUNT(*) AS cnt FROM employees GROUP BY dept_id"
        )
        total = sum(r["cnt"] for r in rows)
        assert total == 50

    def test_aggregate_sum(self):
        rows = self._run("SELECT SUM(salary) AS total FROM employees")
        expected = sum(30_000 + i * 1_000 for i in range(50))
        assert abs(rows[0]["total"] - expected) < 1

    def test_order_by_desc(self):
        rows = self._run("SELECT id, salary FROM employees ORDER BY salary DESC")
        salaries = [r["salary"] for r in rows]
        assert salaries == sorted(salaries, reverse=True)

    def test_limit(self):
        rows = self._run("SELECT * FROM employees LIMIT 7")
        assert len(rows) == 7

    def test_limit_offset(self):
        all_rows = self._run("SELECT id FROM employees ORDER BY id ASC")
        paged = self._run("SELECT id FROM employees ORDER BY id ASC LIMIT 5 OFFSET 10")
        assert [r["id"] for r in paged] == [r["id"] for r in all_rows[10:15]]

    def test_complex_query(self):
        rows = self._run(
            "SELECT d.name, COUNT(*) AS emp_count, AVG(e.salary) AS avg_sal "
            "FROM employees e "
            "JOIN departments d ON e.dept_id = d.id "
            "WHERE e.active = true "
            "GROUP BY d.name "
            "ORDER BY avg_sal DESC "
            "LIMIT 3"
        )
        assert len(rows) <= 3
        assert all("name" in r and "emp_count" in r and "avg_sal" in r for r in rows)

    def test_having(self):
        rows = self._run(
            "SELECT dept_id, COUNT(*) AS cnt FROM employees "
            "GROUP BY dept_id HAVING cnt > 5"
        )
        assert all(r["cnt"] > 5 for r in rows)
