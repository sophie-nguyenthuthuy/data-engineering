"""
Extract candidate materialized views from a query workload.

Strategy:
  1. Fingerprint each query (normalise literals → placeholders).
  2. Group queries whose fingerprints share common sub-trees:
       • CTEs (WITH …)
       • Subqueries in FROM/JOIN
       • Aggregations over the same table + GROUP BY columns
  3. A pattern that appears in ≥ min_frequency queries (weighted by cost) becomes
     a CandidateView.
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

try:
    import sqlglot
    import sqlglot.expressions as exp
    _SQLGLOT = True
except ImportError:
    _SQLGLOT = False

from .models import CandidateView, QueryRecord


# ---------------------------------------------------------------------------
# SQL normalisation helpers
# ---------------------------------------------------------------------------

_LITERAL_RE = re.compile(
    r"'[^']*'"           # single-quoted strings
    r'|"[^"]*"'          # double-quoted strings
    r"|\b\d+\.?\d*\b"   # numbers
)
_WS_RE = re.compile(r"\s+")


def fingerprint(sql: str) -> str:
    normalised = _LITERAL_RE.sub("?", sql.upper())
    normalised = _WS_RE.sub(" ", normalised).strip()
    return hashlib.sha256(normalised.encode()).hexdigest()[:16]


def normalise_sql(sql: str) -> str:
    n = _LITERAL_RE.sub("?", sql.upper())
    return _WS_RE.sub(" ", n).strip()


# ---------------------------------------------------------------------------
# Sub-query extraction
# ---------------------------------------------------------------------------

@dataclass
class _Pattern:
    sql: str
    fp: str
    tables: list[str]
    query_ids: list[str] = field(default_factory=list)
    total_cost_usd: float = 0.0


def _extract_tables_simple(sql: str) -> list[str]:
    """Regex-based fallback when sqlglot is not installed."""
    from_re = re.compile(
        r"\bFROM\s+([\w.`\[\]\"]+)", re.IGNORECASE
    )
    join_re = re.compile(
        r"\bJOIN\s+([\w.`\[\]\"]+)", re.IGNORECASE
    )
    tables: list[str] = []
    for m in from_re.finditer(sql):
        tables.append(m.group(1).strip().strip('`"[]'))
    for m in join_re.finditer(sql):
        tables.append(m.group(1).strip().strip('`"[]'))
    return list(dict.fromkeys(tables))  # dedup, preserve order


def _extract_tables_sqlglot(node: exp.Expression) -> list[str]:
    tables: list[str] = []
    for t in node.find_all(exp.Table):
        parts = [p for p in [t.args.get("db"), t.args.get("table")] if p]
        name = ".".join(str(p) for p in parts)
        if name:
            tables.append(name)
    return list(dict.fromkeys(tables))


def _subquery_sql(node: exp.Expression) -> Optional[str]:
    try:
        return node.sql(dialect="bigquery")
    except Exception:
        return None


class QueryAnalyzer:
    """
    Analyse a workload and emit CandidateViews.

    Parameters
    ----------
    min_query_frequency : int
        A pattern must appear in at least this many (unique) queries to become
        a candidate.
    min_cost_threshold_usd : float
        Total workload cost attributable to a pattern before it is considered.
    max_candidates : int
        Cap on the number of candidates returned (keeps SA tractable).
    """

    def __init__(
        self,
        min_query_frequency: int = 3,
        min_cost_threshold_usd: float = 0.10,
        max_candidates: int = 200,
        dialect: str = "bigquery",
    ) -> None:
        self.min_query_frequency = min_query_frequency
        self.min_cost_threshold_usd = min_cost_threshold_usd
        self.max_candidates = max_candidates
        self.dialect = dialect

    # ------------------------------------------------------------------

    def analyse(self, workload: list[QueryRecord]) -> list[CandidateView]:
        patterns = self._collect_patterns(workload)
        candidates = []
        for pat in patterns.values():
            if len(pat.query_ids) < self.min_query_frequency:
                continue
            if pat.total_cost_usd < self.min_cost_threshold_usd:
                continue
            candidates.append(self._make_candidate(pat))

        # Sort by estimated benefit descending, cap
        candidates.sort(key=lambda c: c.estimated_benefit_usd, reverse=True)
        return candidates[: self.max_candidates]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _collect_patterns(
        self, workload: list[QueryRecord]
    ) -> dict[str, _Pattern]:
        patterns: dict[str, _Pattern] = {}

        for record in workload:
            sub_sqls = self._extract_sub_sqls(record.sql)
            for sql_fragment in sub_sqls:
                fp = fingerprint(sql_fragment)
                if fp not in patterns:
                    patterns[fp] = _Pattern(
                        sql=normalise_sql(sql_fragment),
                        fp=fp,
                        tables=self._extract_tables(sql_fragment),
                    )
                pat = patterns[fp]
                if record.query_id not in pat.query_ids:
                    pat.query_ids.append(record.query_id)
                pat.total_cost_usd += record.cost_usd * record.frequency

        return patterns

    def _extract_sub_sqls(self, sql: str) -> list[str]:
        """Return the top-level query plus any CTEs / subqueries."""
        fragments = [sql]
        if not _SQLGLOT:
            return fragments

        try:
            tree = sqlglot.parse_one(sql, read=self.dialect)
        except Exception:
            return fragments

        # CTEs
        for cte in tree.find_all(exp.CTE):
            s = _subquery_sql(cte.args["this"])
            if s:
                fragments.append(s)

        # Subqueries in FROM / JOIN
        for subq in tree.find_all(exp.Subquery):
            s = _subquery_sql(subq.args["this"])
            if s:
                fragments.append(s)

        return fragments

    def _extract_tables(self, sql: str) -> list[str]:
        if _SQLGLOT:
            try:
                tree = sqlglot.parse_one(sql, read=self.dialect)
                return _extract_tables_sqlglot(tree)
            except Exception:
                pass
        return _extract_tables_simple(sql)

    def _make_candidate(self, pat: _Pattern) -> CandidateView:
        # Very rough size estimate: 1 GB per 10 unique queries referencing it
        est_bytes = max(1, len(pat.query_ids)) * 100 * 1024 * 1024  # 100 MB per query

        # Maintenance ~ 1 % of total attributed cost per refresh cycle
        maintenance = pat.total_cost_usd * 0.01

        # Benefit: fraction of attributed cost we save by materialising
        # Assume 70 % scan reduction (conservative; calibrated later)
        benefit = pat.total_cost_usd * 0.70

        # Stable, human-readable name from first 8 hex chars of fingerprint
        name = f"mv_{pat.fp[:8]}"

        return CandidateView(
            sql=pat.sql,
            name=name,
            view_id=pat.fp,
            referenced_tables=pat.tables,
            benefiting_query_ids=list(pat.query_ids),
            estimated_storage_bytes=est_bytes,
            estimated_maintenance_cost_usd=maintenance,
            estimated_benefit_usd=benefit,
        )
