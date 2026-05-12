"""SQL lexer — converts a SQL string into a flat token stream."""
from __future__ import annotations
import re
from dataclasses import dataclass
from enum import Enum, auto


class TT(Enum):
    """Token type."""
    # Literals
    INTEGER   = auto()
    FLOAT     = auto()
    STRING    = auto()
    TRUE      = auto()
    FALSE     = auto()
    NULL      = auto()
    # Identifiers / keywords
    IDENT     = auto()
    # Keywords (each gets its own type for unambiguous parsing)
    SELECT    = auto()
    FROM      = auto()
    WHERE     = auto()
    JOIN      = auto()
    INNER     = auto()
    LEFT      = auto()
    RIGHT     = auto()
    FULL      = auto()
    OUTER     = auto()
    ON        = auto()
    AND       = auto()
    OR        = auto()
    NOT       = auto()
    AS        = auto()
    GROUP     = auto()
    BY        = auto()
    ORDER     = auto()
    HAVING    = auto()
    LIMIT     = auto()
    OFFSET    = auto()
    ASC       = auto()
    DESC      = auto()
    IS        = auto()
    IN        = auto()
    BETWEEN   = auto()
    LIKE      = auto()
    DISTINCT  = auto()
    STAR      = auto()
    # Aggregate / scalar functions
    COUNT     = auto()
    SUM       = auto()
    AVG       = auto()
    MIN       = auto()
    MAX       = auto()
    # Operators
    EQ        = auto()   # =
    NEQ       = auto()   # != or <>
    LT        = auto()   # <
    LTE       = auto()   # <=
    GT        = auto()   # >
    GTE       = auto()   # >=
    PLUS      = auto()   # +
    MINUS     = auto()   # -
    SLASH     = auto()   # /
    # Punctuation
    COMMA     = auto()
    DOT       = auto()
    LPAREN    = auto()
    RPAREN    = auto()
    SEMICOLON = auto()
    # Control
    EOF       = auto()


_KEYWORDS: dict[str, TT] = {
    "select": TT.SELECT, "from": TT.FROM, "where": TT.WHERE,
    "join": TT.JOIN, "inner": TT.INNER, "left": TT.LEFT,
    "right": TT.RIGHT, "full": TT.FULL, "outer": TT.OUTER,
    "on": TT.ON, "and": TT.AND, "or": TT.OR, "not": TT.NOT,
    "as": TT.AS, "group": TT.GROUP, "by": TT.BY, "order": TT.ORDER,
    "having": TT.HAVING, "limit": TT.LIMIT, "offset": TT.OFFSET,
    "asc": TT.ASC, "desc": TT.DESC, "is": TT.IS, "in": TT.IN,
    "between": TT.BETWEEN, "like": TT.LIKE, "distinct": TT.DISTINCT,
    "true": TT.TRUE, "false": TT.FALSE, "null": TT.NULL,
    "count": TT.COUNT, "sum": TT.SUM, "avg": TT.AVG,
    "min": TT.MIN, "max": TT.MAX,
}

_AGG_FUNCS: set[TT] = {TT.COUNT, TT.SUM, TT.AVG, TT.MIN, TT.MAX}


@dataclass(slots=True)
class Token:
    type: TT
    value: object  # str | int | float | None
    pos: int       # character offset in source

    def __repr__(self) -> str:
        return f"Token({self.type.name}, {self.value!r})"


class LexError(Exception):
    pass


def tokenize(sql: str) -> list[Token]:
    tokens: list[Token] = []
    i = 0
    n = len(sql)

    while i < n:
        # Skip whitespace
        if sql[i].isspace():
            i += 1
            continue

        # Single-line comment
        if sql[i : i + 2] == "--":
            while i < n and sql[i] != "\n":
                i += 1
            continue

        # String literal  'hello'
        if sql[i] == "'":
            j = i + 1
            while j < n and sql[j] != "'":
                if sql[j] == "\\" and j + 1 < n:
                    j += 1
                j += 1
            if j >= n:
                raise LexError(f"Unterminated string at position {i}")
            tokens.append(Token(TT.STRING, sql[i + 1 : j], i))
            i = j + 1
            continue

        # Number
        if sql[i].isdigit() or (sql[i] == "-" and i + 1 < n and sql[i + 1].isdigit()
                                  and (not tokens or tokens[-1].type not in (TT.INTEGER, TT.FLOAT, TT.IDENT, TT.RPAREN))):
            j = i
            if sql[j] == "-":
                j += 1
            while j < n and sql[j].isdigit():
                j += 1
            is_float = j < n and sql[j] == "."
            if is_float:
                j += 1
                while j < n and sql[j].isdigit():
                    j += 1
            raw = sql[i:j]
            tokens.append(Token(TT.FLOAT if is_float else TT.INTEGER,
                                 float(raw) if is_float else int(raw), i))
            i = j
            continue

        # Identifier or keyword
        if sql[i].isalpha() or sql[i] == "_":
            j = i
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            word = sql[i:j]
            tt = _KEYWORDS.get(word.lower(), TT.IDENT)
            tokens.append(Token(tt, word, i))
            i = j
            continue

        # Two-char operators
        two = sql[i : i + 2]
        if two == "!=":
            tokens.append(Token(TT.NEQ, "!=", i)); i += 2; continue
        if two == "<>":
            tokens.append(Token(TT.NEQ, "<>", i)); i += 2; continue
        if two == "<=":
            tokens.append(Token(TT.LTE, "<=", i)); i += 2; continue
        if two == ">=":
            tokens.append(Token(TT.GTE, ">=", i)); i += 2; continue

        # Single-char
        ONE = {
            "=": TT.EQ, "<": TT.LT, ">": TT.GT,
            "+": TT.PLUS, "-": TT.MINUS, "*": TT.STAR, "/": TT.SLASH,
            ",": TT.COMMA, ".": TT.DOT,
            "(": TT.LPAREN, ")": TT.RPAREN, ";": TT.SEMICOLON,
        }
        if sql[i] in ONE:
            tokens.append(Token(ONE[sql[i]], sql[i], i)); i += 1; continue

        raise LexError(f"Unexpected character {sql[i]!r} at position {i}")

    tokens.append(Token(TT.EOF, None, n))
    return tokens


# Re-export for convenience
AGG_FUNCS = _AGG_FUNCS
