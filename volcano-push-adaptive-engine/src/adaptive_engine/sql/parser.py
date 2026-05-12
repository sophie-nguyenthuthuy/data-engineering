"""Recursive-descent SQL parser.

Produces a lightweight SQL AST (plain dataclasses) from a token stream.
The AST is deliberately minimal: only what the planner needs to build
a physical plan.

Supported syntax:
    SELECT [DISTINCT] col_list
    FROM   table [AS alias]
    [INNER | LEFT | RIGHT] JOIN table [AS alias] ON condition
    [WHERE condition]
    [GROUP BY col, ...]
    [HAVING condition]          -- treated as a post-aggregate filter
    [ORDER BY col [ASC|DESC], ...]
    [LIMIT n [OFFSET m]]

Expressions in SELECT:
    *, col, table.col, col AS alias,
    agg_func(col) AS alias, agg_func(*) AS alias

WHERE / ON conditions:
    col op literal               (op: = != <> < <= > >=)
    condition AND condition
    condition OR  condition
    NOT condition
    (condition)
    col IS [NOT] NULL
    col BETWEEN lo AND hi        (desugared to col >= lo AND col <= hi)
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any

from .lexer import TT, AGG_FUNCS, LexError, Token, tokenize


# ------------------------------------------------------------------
# SQL AST nodes
# ------------------------------------------------------------------

@dataclass
class SelectItem:
    expr: "SqlExpr"
    alias: str | None = None


@dataclass
class JoinClause:
    table: str
    alias: str | None
    join_type: str          # "inner" | "left" | "right" | "full"
    condition: "SqlExpr"    # ON condition


@dataclass
class SqlQuery:
    distinct: bool
    select: list[SelectItem]
    from_table: str
    from_alias: str | None
    joins: list[JoinClause]
    where: "SqlExpr | None"
    group_by: list["SqlExpr"]
    having: "SqlExpr | None"
    order_by: list[tuple["SqlExpr", bool]]  # (expr, ascending)
    limit: int | None
    offset: int


# ------------------------------------------------------------------
# SQL expression nodes (separate from the engine's Expr hierarchy)
# ------------------------------------------------------------------

@dataclass
class SqlColRef:
    name: str
    table: str | None = None   # table qualifier

    def __repr__(self) -> str:
        return f"{self.table}.{self.name}" if self.table else self.name


@dataclass
class SqlLiteral:
    value: Any

    def __repr__(self) -> str:
        return repr(self.value)


@dataclass
class SqlBinOp:
    left: "SqlExpr"
    op: str
    right: "SqlExpr"


@dataclass
class SqlUnaryOp:
    op: str   # "NOT" | "-"
    operand: "SqlExpr"


@dataclass
class SqlAgg:
    func: str        # "count" | "sum" | "avg" | "min" | "max"
    arg: "SqlExpr | None"   # None means COUNT(*)


@dataclass
class SqlIsNull:
    expr: "SqlExpr"
    negated: bool


SqlExpr = SqlColRef | SqlLiteral | SqlBinOp | SqlUnaryOp | SqlAgg | SqlIsNull


# ------------------------------------------------------------------
# Errors
# ------------------------------------------------------------------

class ParseError(Exception):
    def __init__(self, msg: str, token: Token | None = None) -> None:
        loc = f" at position {token.pos} ({token!r})" if token else ""
        super().__init__(msg + loc)
        self.token = token


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------

class Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self._tokens = tokens
        self._pos = 0

    @classmethod
    def parse(cls, sql: str) -> SqlQuery:
        tokens = tokenize(sql.strip().rstrip(";"))
        return cls(tokens)._query()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _peek(self) -> Token:
        return self._tokens[self._pos]

    def _advance(self) -> Token:
        tok = self._tokens[self._pos]
        if tok.type != TT.EOF:
            self._pos += 1
        return tok

    def _expect(self, tt: TT) -> Token:
        tok = self._peek()
        if tok.type != tt:
            raise ParseError(f"Expected {tt.name}", tok)
        return self._advance()

    def _match(self, *types: TT) -> bool:
        return self._peek().type in types

    def _consume(self, *types: TT) -> Token | None:
        if self._match(*types):
            return self._advance()
        return None

    def _ident(self) -> str:
        tok = self._peek()
        # Allow keywords as identifiers in table/column position
        if tok.type in (TT.IDENT,) or tok.type in _KEYWORD_AS_IDENT:
            self._advance()
            return str(tok.value)
        raise ParseError("Expected identifier", tok)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def _query(self) -> SqlQuery:
        self._expect(TT.SELECT)

        distinct = bool(self._consume(TT.DISTINCT))
        select_items = self._select_list()

        self._expect(TT.FROM)
        from_table = self._ident()
        from_alias = self._alias()

        joins: list[JoinClause] = []
        while self._match(TT.JOIN, TT.INNER, TT.LEFT, TT.RIGHT, TT.FULL):
            joins.append(self._join_clause())

        where = None
        if self._consume(TT.WHERE):
            where = self._condition()

        group_by: list[SqlExpr] = []
        if self._match(TT.GROUP):
            self._advance()
            self._expect(TT.BY)
            group_by.append(self._expr())
            while self._consume(TT.COMMA):
                group_by.append(self._expr())

        having = None
        if self._consume(TT.HAVING):
            having = self._condition()

        order_by: list[tuple[SqlExpr, bool]] = []
        if self._match(TT.ORDER):
            self._advance()
            self._expect(TT.BY)
            order_by.append(self._order_item())
            while self._consume(TT.COMMA):
                order_by.append(self._order_item())

        limit: int | None = None
        offset: int = 0
        if self._consume(TT.LIMIT):
            limit = int(self._expect(TT.INTEGER).value)  # type: ignore[arg-type]
        if self._consume(TT.OFFSET):
            offset = int(self._expect(TT.INTEGER).value)  # type: ignore[arg-type]

        return SqlQuery(
            distinct=distinct,
            select=select_items,
            from_table=from_table,
            from_alias=from_alias,
            joins=joins,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    # ------------------------------------------------------------------
    # SELECT list
    # ------------------------------------------------------------------

    def _select_list(self) -> list[SelectItem]:
        items = [self._select_item()]
        while self._consume(TT.COMMA):
            items.append(self._select_item())
        return items

    def _select_item(self) -> SelectItem:
        if self._match(TT.STAR):
            self._advance()
            return SelectItem(expr=SqlLiteral("*"))

        # Aggregate function
        if self._peek().type in AGG_FUNCS:
            func = self._advance().type.name.lower()
            self._expect(TT.LPAREN)
            if self._match(TT.STAR):
                self._advance()
                arg = None
            else:
                arg = self._expr()
            self._expect(TT.RPAREN)
            expr: SqlExpr = SqlAgg(func=func, arg=arg)
        else:
            expr = self._expr()

        alias = None
        if self._consume(TT.AS):
            alias = self._ident()
        elif self._match(TT.IDENT):
            # Implicit alias: col newname (without AS)
            alias = self._ident()

        return SelectItem(expr=expr, alias=alias)

    # ------------------------------------------------------------------
    # JOIN
    # ------------------------------------------------------------------

    def _join_clause(self) -> JoinClause:
        join_type = "inner"
        if self._match(TT.LEFT):
            self._advance(); self._consume(TT.OUTER); join_type = "left"
        elif self._match(TT.RIGHT):
            self._advance(); self._consume(TT.OUTER); join_type = "right"
        elif self._match(TT.FULL):
            self._advance(); self._consume(TT.OUTER); join_type = "full"
        elif self._consume(TT.INNER):
            pass
        self._expect(TT.JOIN)
        table = self._ident()
        alias = self._alias()
        self._expect(TT.ON)
        cond = self._condition()
        return JoinClause(table=table, alias=alias, join_type=join_type, condition=cond)

    # ------------------------------------------------------------------
    # ORDER BY item
    # ------------------------------------------------------------------

    def _order_item(self) -> tuple[SqlExpr, bool]:
        expr = self._expr()
        ascending = True
        if self._consume(TT.DESC):
            ascending = False
        elif self._consume(TT.ASC):
            ascending = True
        return (expr, ascending)

    # ------------------------------------------------------------------
    # Alias
    # ------------------------------------------------------------------

    def _alias(self) -> str | None:
        if self._consume(TT.AS):
            return self._ident()
        # Implicit alias: bare identifier that is not a keyword
        if self._peek().type == TT.IDENT:
            return self._ident()
        return None

    # ------------------------------------------------------------------
    # Expressions / conditions
    # ------------------------------------------------------------------

    def _condition(self) -> SqlExpr:
        return self._or_expr()

    def _or_expr(self) -> SqlExpr:
        left = self._and_expr()
        while self._consume(TT.OR):
            right = self._and_expr()
            left = SqlBinOp(left, "OR", right)
        return left

    def _and_expr(self) -> SqlExpr:
        left = self._not_expr()
        while self._consume(TT.AND):
            right = self._not_expr()
            left = SqlBinOp(left, "AND", right)
        return left

    def _not_expr(self) -> SqlExpr:
        if self._consume(TT.NOT):
            return SqlUnaryOp("NOT", self._not_expr())
        return self._comparison()

    def _comparison(self) -> SqlExpr:
        if self._match(TT.LPAREN):
            self._advance()
            expr = self._condition()
            self._expect(TT.RPAREN)
            return expr

        left = self._expr()

        # IS [NOT] NULL
        if self._consume(TT.IS):
            negated = bool(self._consume(TT.NOT))
            self._expect(TT.NULL)
            return SqlIsNull(left, negated)

        # BETWEEN lo AND hi
        if self._consume(TT.BETWEEN):
            lo = self._expr()
            self._expect(TT.AND)
            hi = self._expr()
            return SqlBinOp(
                SqlBinOp(left, ">=", lo),
                "AND",
                SqlBinOp(left, "<=", hi),
            )

        # Standard comparison
        op_map = {
            TT.EQ: "=", TT.NEQ: "!=", TT.LT: "<",
            TT.LTE: "<=", TT.GT: ">", TT.GTE: ">=",
        }
        if self._peek().type in op_map:
            op = op_map[self._advance().type]
            right = self._expr()
            return SqlBinOp(left, op, right)

        return left

    def _expr(self) -> SqlExpr:
        """Arithmetic expression (add/sub level)."""
        left = self._term()
        while self._match(TT.PLUS, TT.MINUS):
            op = self._advance().value
            right = self._term()
            left = SqlBinOp(left, op, right)
        return left

    def _term(self) -> SqlExpr:
        """Arithmetic expression (mul/div level)."""
        left = self._primary()
        while self._match(TT.STAR, TT.SLASH):
            op = self._advance().value
            right = self._primary()
            left = SqlBinOp(left, op, right)
        return left

    def _primary(self) -> SqlExpr:
        tok = self._peek()

        if tok.type == TT.INTEGER:
            self._advance(); return SqlLiteral(int(tok.value))  # type: ignore[arg-type]
        if tok.type == TT.FLOAT:
            self._advance(); return SqlLiteral(float(tok.value))  # type: ignore[arg-type]
        if tok.type == TT.STRING:
            self._advance(); return SqlLiteral(str(tok.value))
        if tok.type == TT.TRUE:
            self._advance(); return SqlLiteral(True)
        if tok.type == TT.FALSE:
            self._advance(); return SqlLiteral(False)
        if tok.type == TT.NULL:
            self._advance(); return SqlLiteral(None)

        if tok.type == TT.LPAREN:
            self._advance()
            expr = self._condition()
            self._expect(TT.RPAREN)
            return expr

        if tok.type == TT.MINUS:
            self._advance()
            operand = self._primary()
            return SqlUnaryOp("-", operand)

        # Aggregate function
        if tok.type in AGG_FUNCS:
            func = self._advance().type.name.lower()
            self._expect(TT.LPAREN)
            if self._match(TT.STAR):
                self._advance(); arg = None
            else:
                arg = self._expr()
            self._expect(TT.RPAREN)
            return SqlAgg(func=func, arg=arg)

        # Column reference (possibly table-qualified)
        if tok.type == TT.IDENT or tok.type in _KEYWORD_AS_IDENT:
            name = self._ident()
            if self._consume(TT.DOT):
                col = self._ident()
                return SqlColRef(name=col, table=name)
            return SqlColRef(name=name)

        raise ParseError(f"Unexpected token in expression", tok)


# Keywords that are also valid identifiers in column/table position
_KEYWORD_AS_IDENT: set[TT] = {
    TT.COUNT, TT.SUM, TT.AVG, TT.MIN, TT.MAX,
    TT.GROUP, TT.ORDER, TT.LIMIT, TT.OFFSET,
    TT.ASC, TT.DESC, TT.HAVING, TT.DISTINCT,
}
