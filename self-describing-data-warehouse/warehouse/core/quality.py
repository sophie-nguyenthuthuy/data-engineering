"""
Quality scorer — runs checks against actual data tables and records results
in the metadata layer.  Score 0-100 based on completeness, uniqueness,
constraint validity, and trend stability.
"""

import sqlite3
from datetime import datetime, timezone
from typing import Optional


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class QualityScorer:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def run(self, table_name: str, notes: str = "") -> dict:
        """
        Inspect the actual table and compute a quality score.
        Returns the full quality run result dict.
        """
        try:
            row_count = self._row_count(table_name)
        except Exception:
            row_count = 0

        null_rate = self._null_rate(table_name) if row_count > 0 else 1.0
        dup_rate = self._duplicate_rate(table_name) if row_count > 0 else 0.0
        violations = self._constraint_violations(table_name)

        score = self._compute_score(null_rate, dup_rate, violations, row_count)

        self.conn.execute(
            """
            INSERT INTO meta_quality_runs
                (table_name, run_at, row_count, null_rate, duplicate_rate,
                 constraint_violations, quality_score, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (table_name, _now(), row_count, null_rate, dup_rate, violations, score, notes),
        )
        self.conn.commit()

        return {
            "table_name": table_name,
            "row_count": row_count,
            "null_rate": round(null_rate, 4),
            "duplicate_rate": round(dup_rate, 4),
            "constraint_violations": violations,
            "quality_score": round(score, 2),
        }

    def latest(self, table_name: str) -> Optional[dict]:
        row = self.conn.execute(
            """
            SELECT * FROM meta_quality_runs
            WHERE table_name=?
            ORDER BY run_at DESC LIMIT 1
            """,
            (table_name,),
        ).fetchone()
        return dict(row) if row else None

    def history(self, table_name: str, limit: int = 10) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT * FROM meta_quality_runs
            WHERE table_name=?
            ORDER BY run_at DESC LIMIT ?
            """,
            (table_name, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def trend(self, table_name: str) -> str:
        """Return 'improving', 'degrading', or 'stable' based on last 5 runs."""
        rows = self.conn.execute(
            """
            SELECT quality_score FROM meta_quality_runs
            WHERE table_name=? ORDER BY run_at DESC LIMIT 5
            """,
            (table_name,),
        ).fetchall()
        scores = [r["quality_score"] for r in rows]
        if len(scores) < 2:
            return "stable"
        delta = scores[0] - scores[-1]
        if delta > 3:
            return "improving"
        elif delta < -3:
            return "degrading"
        return "stable"

    # --- private helpers ---

    def _row_count(self, table_name: str) -> int:
        row = self.conn.execute(f"SELECT COUNT(*) as n FROM [{table_name}]").fetchone()
        return row["n"]

    def _null_rate(self, table_name: str) -> float:
        """Average null rate across all columns."""
        cols = self.conn.execute(
            f"PRAGMA table_info([{table_name}])"
        ).fetchall()
        if not cols:
            return 0.0
        total_rate = 0.0
        total_count = self._row_count(table_name)
        if total_count == 0:
            return 0.0
        for col in cols:
            col_name = col["name"]
            null_count = self.conn.execute(
                f"SELECT COUNT(*) as n FROM [{table_name}] WHERE [{col_name}] IS NULL"
            ).fetchone()["n"]
            total_rate += null_count / total_count
        return total_rate / len(cols)

    def _duplicate_rate(self, table_name: str) -> float:
        total = self._row_count(table_name)
        if total == 0:
            return 0.0
        cols = self.conn.execute(f"PRAGMA table_info([{table_name}])").fetchall()
        col_names = ", ".join(f"[{c['name']}]" for c in cols)
        distinct = self.conn.execute(
            f"SELECT COUNT(*) as n FROM (SELECT DISTINCT {col_names} FROM [{table_name}])"
        ).fetchone()["n"]
        return (total - distinct) / total

    def _constraint_violations(self, table_name: str) -> int:
        """Count rows where NOT NULL columns are violated (catches inserted nulls)."""
        cols = self.conn.execute(f"PRAGMA table_info([{table_name}])").fetchall()
        violations = 0
        for col in cols:
            if col["notnull"] == 1:
                n = self.conn.execute(
                    f"SELECT COUNT(*) as n FROM [{table_name}] WHERE [{col['name']}] IS NULL"
                ).fetchone()["n"]
                violations += n
        return violations

    def _compute_score(
        self, null_rate: float, dup_rate: float, violations: int, row_count: int
    ) -> float:
        completeness = max(0, 100 - null_rate * 100)
        uniqueness = max(0, 100 - dup_rate * 100)
        validity = 100 if violations == 0 else max(0, 100 - (violations / max(row_count, 1)) * 100)
        return round(completeness * 0.4 + uniqueness * 0.4 + validity * 0.2, 2)
