"""Lightweight SQL filter / group / join extractor.

We do NOT implement a full SQL parser — production deployments swap in
``sqlglot`` for that. The advisor only needs to know:

  * which columns appear in a ``WHERE`` clause (filter candidates),
  * which appear in ``JOIN ... ON`` predicates (bucket candidates),
  * which appear in ``GROUP BY`` (pre-aggregate hints).

These three signals dominate partition / bucket recommendations.

The parser tokenises whitespace-separated SQL, then walks the token
stream extracting identifiers around the keywords above. It handles
the common shapes (``a = b``, ``a IN (...)``, ``a BETWEEN x AND y``,
``date >= '2024-01-01'``) but won't pretend to understand subqueries
or CTEs — the docstring tells callers to drop those upstream.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_TOKEN_RE = re.compile(
    r"""
    (?P<str>'[^']*')
  | (?P<num>\b\d+(?:\.\d+)?\b)
  | (?P<id>[A-Za-z_][A-Za-z0-9_.]*)
  | (?P<op>=|<>|!=|<=|>=|<|>|\(|\)|,)
""",
    re.VERBOSE,
)


_KEYWORDS = {
    "select",
    "from",
    "where",
    "and",
    "or",
    "in",
    "between",
    "join",
    "left",
    "right",
    "inner",
    "outer",
    "full",
    "on",
    "group",
    "by",
    "order",
    "limit",
    "having",
    "as",
    "not",
    "null",
    "is",
    "asc",
    "desc",
    "true",
    "false",
}


def _tokenise(sql: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for match in _TOKEN_RE.finditer(sql):
        kind = match.lastgroup
        text = match.group()
        if kind == "id" and text.lower() in _KEYWORDS:
            out.append(("kw", text.lower()))
        else:
            out.append((kind or "op", text))
    return out


def _column_name(raw: str) -> str:
    """Strip any table-qualifier prefix (``t.col`` → ``col``)."""
    return raw.split(".")[-1].lower()


@dataclass(frozen=True, slots=True)
class ParsedQuery:
    """Identifiers extracted from one SQL statement."""

    raw: str
    filter_columns: tuple[str, ...] = field(default_factory=tuple)
    join_columns: tuple[str, ...] = field(default_factory=tuple)
    group_columns: tuple[str, ...] = field(default_factory=tuple)


def parse_query(sql: str) -> ParsedQuery:
    if not sql.strip():
        raise ValueError("sql must be non-empty")
    tokens = _tokenise(sql)
    filter_cols: list[str] = []
    join_cols: list[str] = []
    group_cols: list[str] = []
    section: str | None = None
    i = 0
    while i < len(tokens):
        kind, text = tokens[i]
        lower = text.lower()
        if kind == "kw":
            if lower == "where":
                section = "where"
                i += 1
                continue
            if lower == "on":
                section = "on"
                i += 1
                continue
            if lower == "group" and i + 1 < len(tokens) and tokens[i + 1][1].lower() == "by":
                section = "group"
                i += 2
                continue
            if lower in {"order", "limit", "having"}:
                section = None
                i += 1
                continue
            if lower in {"and", "or", "not"} or lower in {"in", "between", "is", "null"}:
                i += 1
                continue
            if lower in {"join", "left", "right", "inner", "outer", "full"}:
                i += 1
                continue
            if lower == "from":
                section = None
                i += 1
                continue
        if section == "where" and kind == "id":
            # Filter columns: anything immediately before a comparison op.
            if (
                (
                    i + 1 < len(tokens)
                    and tokens[i + 1][0] == "op"
                    and tokens[i + 1][1] in {"=", "<>", "!=", "<", "<=", ">", ">="}
                )
                or (i + 1 < len(tokens) and tokens[i + 1] == ("kw", "in"))
                or (i + 1 < len(tokens) and tokens[i + 1] == ("kw", "between"))
                or (i + 1 < len(tokens) and tokens[i + 1] == ("kw", "is"))
            ):
                filter_cols.append(_column_name(text))
        elif section == "on" and kind == "id":
            # JOIN keys: both sides of `=`.
            if i + 1 < len(tokens) and tokens[i + 1] == ("op", "="):
                join_cols.append(_column_name(text))
                if i + 2 < len(tokens) and tokens[i + 2][0] == "id":
                    join_cols.append(_column_name(tokens[i + 2][1]))
        elif section == "group" and kind == "id":
            group_cols.append(_column_name(text))
        i += 1
    return ParsedQuery(
        raw=sql,
        filter_columns=tuple(dict.fromkeys(filter_cols)),  # preserve order, dedupe
        join_columns=tuple(dict.fromkeys(join_cols)),
        group_columns=tuple(dict.fromkeys(group_cols)),
    )


__all__ = ["ParsedQuery", "parse_query"]
