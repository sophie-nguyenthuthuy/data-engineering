"""
Table recommender — answers "which table should I use for X?" by ranking
all registered tables on relevance, quality, freshness, usage, and reliability.
"""

import re
import sqlite3
from dataclasses import dataclass
from typing import Optional

from warehouse.core.quality import QualityScorer
from warehouse.core.freshness import FreshnessMonitor
from warehouse.core.usage import UsageTracker
from warehouse.core.incidents import IncidentTracker


@dataclass
class Recommendation:
    table_name: str
    description: str
    domain: str
    owner: str
    relevance_score: float
    quality_score: float
    freshness_score: float
    usage_score: float
    reliability_score: float
    composite_score: float
    tags: list
    is_deprecated: bool
    deprecation_note: str

    def __str__(self) -> str:
        bar = "█" * int(self.composite_score / 10)
        status = " [DEPRECATED]" if self.is_deprecated else ""
        return (
            f"  {self.table_name}{status}\n"
            f"    Score: {self.composite_score:.1f}/100  {bar}\n"
            f"    {self.description}\n"
            f"    Owner: {self.owner} | Domain: {self.domain}\n"
            f"    Quality:{self.quality_score:.0f}  "
            f"Freshness:{self.freshness_score:.0f}  "
            f"Usage:{self.usage_score:.0f}  "
            f"Reliability:{self.reliability_score:.0f}  "
            f"Relevance:{self.relevance_score:.0f}\n"
        )


class TableRecommender:
    WEIGHTS = {
        "relevance":   0.35,
        "quality":     0.25,
        "freshness":   0.20,
        "usage":       0.10,
        "reliability": 0.10,
    }

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.quality = QualityScorer(conn)
        self.freshness = FreshnessMonitor(conn)
        self.usage = UsageTracker(conn)
        self.incidents = IncidentTracker(conn)

    def recommend(
        self,
        query: str,
        domain: Optional[str] = None,
        top_k: int = 5,
        include_deprecated: bool = False,
    ) -> list[Recommendation]:
        """
        Return the top-k tables most relevant to the natural-language query.
        """
        sql = "SELECT * FROM meta_tables WHERE 1=1"
        params: list = []
        if domain:
            sql += " AND domain=?"
            params.append(domain)
        if not include_deprecated:
            sql += " AND is_deprecated=0"
        rows = self.conn.execute(sql, params).fetchall()

        results = []
        for row in rows:
            t = dict(row)
            rel = self._relevance(query, t)
            quality = self._latest_quality(t["table_name"])
            fresh = self._latest_freshness(t["table_name"])
            usage = self.usage.usage_score(t["table_name"])
            reliability = self.incidents.reliability_score(t["table_name"])
            composite = (
                rel        * self.WEIGHTS["relevance"]
                + quality  * self.WEIGHTS["quality"]
                + fresh    * self.WEIGHTS["freshness"]
                + usage    * self.WEIGHTS["usage"]
                + reliability * self.WEIGHTS["reliability"]
            )
            import json
            results.append(
                Recommendation(
                    table_name=t["table_name"],
                    description=t["description"],
                    domain=t["domain"],
                    owner=t["owner"],
                    relevance_score=round(rel, 1),
                    quality_score=round(quality, 1),
                    freshness_score=round(fresh, 1),
                    usage_score=round(usage, 1),
                    reliability_score=round(reliability, 1),
                    composite_score=round(composite, 1),
                    tags=json.loads(t.get("tags") or "[]"),
                    is_deprecated=bool(t.get("is_deprecated")),
                    deprecation_note=t.get("deprecation_note") or "",
                )
            )

        results.sort(key=lambda r: r.composite_score, reverse=True)
        return results[:top_k]

    def _relevance(self, query: str, table: dict) -> float:
        """
        Keyword-overlap relevance between query and table metadata.
        Simple but effective for demonstration; swap in a vector store for prod.
        """
        tokens = set(re.findall(r"\w+", query.lower()))
        searchable = " ".join(
            filter(None, [
                table.get("table_name", ""),
                table.get("description", ""),
                table.get("domain", ""),
                table.get("source_system", ""),
                table.get("tags", ""),
            ])
        ).lower()
        col_rows = self.conn.execute(
            "SELECT column_name, description FROM meta_columns WHERE table_name=?",
            (table["table_name"],),
        ).fetchall()
        for c in col_rows:
            searchable += f" {c['column_name']} {c['description'] or ''}"

        words = set(re.findall(r"\w+", searchable))
        overlap = len(tokens & words)
        return min(100.0, overlap * 25)

    def _latest_quality(self, table_name: str) -> float:
        row = self.conn.execute(
            "SELECT quality_score FROM meta_quality_runs WHERE table_name=? ORDER BY run_at DESC LIMIT 1",
            (table_name,),
        ).fetchone()
        return row["quality_score"] if row else 50.0   # neutral default

    def _latest_freshness(self, table_name: str) -> float:
        row = self.conn.execute(
            "SELECT freshness_score FROM meta_freshness WHERE table_name=? ORDER BY checked_at DESC LIMIT 1",
            (table_name,),
        ).fetchone()
        return row["freshness_score"] if row else 50.0
