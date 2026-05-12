"""Builds a logical query plan from a parsed SQL AST (sqlglot ≥ 23)."""

from __future__ import annotations

from typing import Any

import sqlglot
import sqlglot.expressions as exp

from ..catalog import SchemaCatalog
from .nodes import (
    Aggregate, Filter, Join, JoinType, Limit, PlanNode,
    Project, Sort, TableScan,
)


class QueryPlanner:
    """Translates a sqlglot AST into a tree of PlanNodes."""

    def __init__(self, catalog: SchemaCatalog) -> None:
        self.catalog = catalog

    def build(self, sql: str) -> PlanNode:
        stmt = sqlglot.parse_one(sql)
        if stmt is None or not isinstance(stmt, exp.Select):
            raise ValueError("Only SELECT statements are supported")
        return self._build_select(stmt)

    # ------------------------------------------------------------------ #
    # Top-level builder                                                    #
    # ------------------------------------------------------------------ #

    def _build_select(self, stmt: exp.Select) -> PlanNode:
        # 1. Build TableScans for every FROM / JOIN table
        scans, alias_map = self._collect_scans(stmt)

        # 2. Build join tree
        node: PlanNode = self._build_join_tree(stmt, scans, alias_map)

        # 3. Residual WHERE predicates (cross-table; can't push to one source)
        for pred in self._remaining_predicates(stmt, alias_map):
            node = Filter(child=node, predicate=pred)

        # 4. GROUP BY / aggregates
        group = stmt.args.get("group")
        agg_fns = [
            e for e in stmt.expressions
            if isinstance(e, (exp.Sum, exp.Avg, exp.Count, exp.Max, exp.Min, exp.Anonymous))
        ]
        if group or agg_fns:
            keys = group.expressions if group else []
            node = Aggregate(child=node, group_keys=keys, aggregates=agg_fns)

        # 5. ORDER BY
        order = stmt.args.get("order")
        if order:
            node = Sort(child=node, order_exprs=order.expressions)

        # 6. LIMIT / OFFSET  — in sqlglot ≥ 23 the value is in .args["expression"]
        limit_node = stmt.args.get("limit")
        offset_node = stmt.args.get("offset")
        if limit_node:
            count = self._extract_int(limit_node)
            offset = self._extract_int(offset_node) if offset_node else 0
            node = Limit(child=node, count=count, offset=offset)

        # 7. Final projection
        node = Project(
            child=node,
            columns=stmt.expressions,
            output_names=self._resolve_output_names(stmt.expressions),
        )
        return node

    # ------------------------------------------------------------------ #
    # Scan collection                                                      #
    # ------------------------------------------------------------------ #

    def _collect_scans(
        self, stmt: exp.Select
    ) -> tuple[dict[str, TableScan], dict[str, str]]:
        """Return {alias: TableScan} and {alias: qualified_name}."""
        scans: dict[str, TableScan] = {}
        alias_map: dict[str, str] = {}

        def _add(table_expr: exp.Table) -> None:
            db = table_expr.args.get("db")
            name = table_expr.name
            alias = table_expr.alias or name

            source = db.name if db else self._infer_source(name)
            qualified = f"{source}.{name}"

            schema = self.catalog.get_table(qualified)
            predicates = self._collect_pushed_predicates(stmt, alias, schema)
            projected = self._collect_projected_columns(stmt, alias, schema)

            scan = TableScan(
                source=source,
                table=name,
                alias=alias,
                projected_columns=projected,
                pushed_predicates=predicates,
                estimated_rows=self._estimate_after_filter(schema, predicates),
            )
            if alias not in scans:          # avoid double-adding FROM table
                scans[alias] = scan
                alias_map[alias] = qualified

        # FROM clause — key is "from_" in sqlglot ≥ 23
        from_clause = stmt.args.get("from_") or stmt.args.get("from")
        if from_clause:
            for tbl in from_clause.find_all(exp.Table):
                _add(tbl)

        # JOIN clauses — "joins" is a list in sqlglot ≥ 23
        for join in (stmt.args.get("joins") or []):
            tbl = join.this
            if isinstance(tbl, exp.Table):
                _add(tbl)

        return scans, alias_map

    # ------------------------------------------------------------------ #
    # Join tree                                                            #
    # ------------------------------------------------------------------ #

    def _build_join_tree(
        self,
        stmt: exp.Select,
        scans: dict[str, TableScan],
        alias_map: dict[str, str],
    ) -> PlanNode:
        aliases = list(scans.keys())
        if not aliases:
            raise ValueError("No tables found in query")

        node: PlanNode = scans[aliases[0]]

        for join in (stmt.args.get("joins") or []):
            tbl = join.this
            if not isinstance(tbl, exp.Table):
                continue
            right_alias = tbl.alias or tbl.name
            right_node = scans.get(right_alias)
            if right_node is None:
                continue

            join_type = self._map_join_type(join)
            # In sqlglot ≥ 23 the ON condition sits directly in join.args["on"]
            condition = join.args.get("on")
            left_keys, right_keys = self._extract_join_keys(condition, right_alias)

            est = max(1, int((node.estimated_rows * right_node.estimated_rows) ** 0.5))
            node = Join(
                left=node,
                right=right_node,
                join_type=join_type,
                condition=condition,
                left_keys=left_keys,
                right_keys=right_keys,
                estimated_rows=est,
            )

        return node

    def _map_join_type(self, join: exp.Join) -> JoinType:
        kind = (join.args.get("kind") or "").upper()
        side = (join.args.get("side") or "").upper()
        if kind == "CROSS":
            return JoinType.CROSS
        if side == "LEFT":
            return JoinType.LEFT
        if side == "RIGHT":
            return JoinType.RIGHT
        if side == "FULL":
            return JoinType.FULL
        return JoinType.INNER

    def _extract_join_keys(
        self, condition: exp.Expression | None, right_alias: str
    ) -> tuple[list[str], list[str]]:
        if condition is None:
            return [], []
        left_keys, right_keys = [], []
        for eq in condition.find_all(exp.EQ):
            lhs, rhs = eq.this, eq.expression
            if not (isinstance(lhs, exp.Column) and isinstance(rhs, exp.Column)):
                continue
            if lhs.table == right_alias:
                lhs, rhs = rhs, lhs
            left_keys.append(f"{lhs.table}.{lhs.name}" if lhs.table else lhs.name)
            right_keys.append(f"{rhs.table}.{rhs.name}" if rhs.table else rhs.name)
        return left_keys, right_keys

    # ------------------------------------------------------------------ #
    # Predicate helpers                                                    #
    # ------------------------------------------------------------------ #

    def _collect_pushed_predicates(
        self, stmt: exp.Select, alias: str, schema: Any
    ) -> list[exp.Expression]:
        """WHERE predicates that reference only this table → push to source."""
        where = stmt.args.get("where")
        if not where:
            return []
        pushed: list[exp.Expression] = []
        for pred in self._split_conjuncts(where.this):
            tables_refs = {c.table for c in pred.find_all(exp.Column) if c.table}
            # Push if all column refs are to this alias, or unqualified
            if not tables_refs or tables_refs <= {alias}:
                pushed.append(pred)
        return pushed

    def _remaining_predicates(
        self, stmt: exp.Select, alias_map: dict[str, str]
    ) -> list[exp.Expression]:
        """Predicates that span multiple tables — must be applied after join."""
        where = stmt.args.get("where")
        if not where:
            return []
        all_aliases = set(alias_map.keys())
        remaining: list[exp.Expression] = []
        for pred in self._split_conjuncts(where.this):
            tables_refs = {c.table for c in pred.find_all(exp.Column) if c.table}
            if len(tables_refs & all_aliases) > 1:
                remaining.append(pred)
        return remaining

    def _split_conjuncts(self, expr: exp.Expression) -> list[exp.Expression]:
        if isinstance(expr, exp.And):
            return self._split_conjuncts(expr.this) + self._split_conjuncts(expr.expression)
        return [expr]

    # ------------------------------------------------------------------ #
    # Projection helpers                                                   #
    # ------------------------------------------------------------------ #

    def _collect_projected_columns(
        self, stmt: exp.Select, alias: str, schema: Any
    ) -> list[str]:
        needed: set[str] = set()
        for col in stmt.find_all(exp.Column):
            if col.table in (alias, "") or not col.table:
                if col.name:
                    needed.add(col.name)
        schema_cols = set(schema.column_names())
        result = list(needed & schema_cols)
        return result if result else schema.column_names()

    def _resolve_output_names(self, exprs: list[exp.Expression]) -> list[str]:
        names: list[str] = []
        for e in exprs:
            if isinstance(e, exp.Alias):
                names.append(e.alias)
            elif isinstance(e, exp.Column):
                names.append(e.name)
            elif isinstance(e, exp.Star):
                names.append("*")
            else:
                names.append(str(e))
        return names

    # ------------------------------------------------------------------ #
    # Cost / utility                                                       #
    # ------------------------------------------------------------------ #

    def _estimate_after_filter(self, schema: Any, predicates: list[Any]) -> int:
        rows = schema.estimated_rows
        for pred in predicates:
            if isinstance(pred, exp.EQ):
                rows = max(1, int(rows * 0.1))
            elif isinstance(pred, (exp.GT, exp.GTE, exp.LT, exp.LTE)):
                rows = max(1, int(rows * 0.3))
            elif isinstance(pred, exp.Like):
                rows = max(1, int(rows * 0.2))
            else:
                rows = max(1, int(rows * 0.5))
        return rows

    def _infer_source(self, table_name: str) -> str:
        for qualified in self.catalog.list_tables():
            src, tbl = qualified.split(".", 1)
            if tbl == table_name:
                return src
        raise KeyError(f"Cannot resolve source for table: {table_name!r}")

    @staticmethod
    def _extract_int(node: exp.Expression) -> int:
        """Extract integer value from a Limit/Offset node across sqlglot versions."""
        # sqlglot ≥ 23: value in node.args["expression"]
        val = node.args.get("expression")
        if val is not None and hasattr(val, "this"):
            try:
                return int(val.this)
            except (TypeError, ValueError):
                pass
        # Older: node.this.this
        if hasattr(node, "this") and node.this is not None:
            try:
                return int(node.this.this)
            except (TypeError, ValueError):
                pass
        return 0
