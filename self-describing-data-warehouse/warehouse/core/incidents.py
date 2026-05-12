"""
Incident tracker — records what broke a table and when it was fixed.
"""

import sqlite3
from datetime import datetime, timezone
from typing import Optional


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


SEVERITIES = ("low", "medium", "high", "critical")


class IncidentTracker:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def open(
        self,
        table_name: str,
        description: str,
        severity: str = "medium",
    ) -> int:
        assert severity in SEVERITIES, f"severity must be one of {SEVERITIES}"
        cur = self.conn.execute(
            """
            INSERT INTO meta_incidents
                (table_name, occurred_at, severity, description)
            VALUES (?, ?, ?, ?)
            """,
            (table_name, _now(), severity, description),
        )
        self.conn.commit()
        return cur.lastrowid

    def resolve(
        self,
        incident_id: int,
        root_cause: str = "",
        resolved_by: str = "",
    ) -> None:
        self.conn.execute(
            """
            UPDATE meta_incidents
            SET resolved_at=?, root_cause=?, resolved_by=?
            WHERE id=?
            """,
            (_now(), root_cause, resolved_by, incident_id),
        )
        self.conn.commit()

    def open_incidents(self, table_name: Optional[str] = None) -> list[dict]:
        if table_name:
            rows = self.conn.execute(
                "SELECT * FROM meta_incidents WHERE table_name=? AND resolved_at IS NULL ORDER BY occurred_at DESC",
                (table_name,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM meta_incidents WHERE resolved_at IS NULL ORDER BY occurred_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def history(self, table_name: str, limit: int = 10) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM meta_incidents WHERE table_name=? ORDER BY occurred_at DESC LIMIT ?",
            (table_name, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def last_incident(self, table_name: str) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM meta_incidents WHERE table_name=? ORDER BY occurred_at DESC LIMIT 1",
            (table_name,),
        ).fetchone()
        return dict(row) if row else None

    def reliability_score(self, table_name: str) -> float:
        """
        0-100.  Penalises tables with recent or unresolved incidents.
        """
        open_inc = self.open_incidents(table_name)
        last = self.last_incident(table_name)
        score = 100.0
        score -= len(open_inc) * 20          # -20 per open incident
        if last:
            try:
                occ = datetime.fromisoformat(last["occurred_at"])
                if occ.tzinfo is None:
                    occ = occ.replace(tzinfo=timezone.utc)
                days_ago = (datetime.now(timezone.utc) - occ).days
                recency_penalty = max(0, 15 - days_ago)
                score -= recency_penalty
            except ValueError:
                pass
        return max(0.0, score)
