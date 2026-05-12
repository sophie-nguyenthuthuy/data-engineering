"""
Freshness monitor — checks how current each table's data is and scores it.
Score 0-100: 100 = updated within expected interval, decays exponentially past deadline.
"""

import sqlite3
from datetime import datetime, timezone
from typing import Optional


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class FreshnessMonitor:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def record(
        self,
        table_name: str,
        last_updated_at: str,
        expected_interval_hours: float,
    ) -> dict:
        checked_at = _now()
        now_dt = datetime.now(timezone.utc)

        try:
            last_dt = datetime.fromisoformat(last_updated_at)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            hours_since = (now_dt - last_dt).total_seconds() / 3600
        except ValueError:
            hours_since = expected_interval_hours * 10

        score = self._score(hours_since, expected_interval_hours)

        self.conn.execute(
            """
            INSERT INTO meta_freshness
                (table_name, checked_at, last_updated_at, expected_interval_hours,
                 hours_since_update, freshness_score)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                table_name,
                checked_at,
                last_updated_at,
                expected_interval_hours,
                round(hours_since, 2),
                round(score, 2),
            ),
        )
        self.conn.commit()

        return {
            "table_name": table_name,
            "last_updated_at": last_updated_at,
            "hours_since_update": round(hours_since, 2),
            "expected_interval_hours": expected_interval_hours,
            "freshness_score": round(score, 2),
            "status": self._status(hours_since, expected_interval_hours),
        }

    def latest(self, table_name: str) -> Optional[dict]:
        row = self.conn.execute(
            """
            SELECT * FROM meta_freshness
            WHERE table_name=?
            ORDER BY checked_at DESC LIMIT 1
            """,
            (table_name,),
        ).fetchone()
        return dict(row) if row else None

    def stale_tables(self, threshold_score: float = 60.0) -> list[dict]:
        """Return all tables with freshness score below threshold."""
        rows = self.conn.execute(
            """
            SELECT f.* FROM meta_freshness f
            INNER JOIN (
                SELECT table_name, MAX(checked_at) as max_checked
                FROM meta_freshness GROUP BY table_name
            ) latest ON f.table_name=latest.table_name AND f.checked_at=latest.max_checked
            WHERE f.freshness_score < ?
            ORDER BY f.freshness_score ASC
            """,
            (threshold_score,),
        ).fetchall()
        return [dict(r) for r in rows]

    def _score(self, hours_since: float, expected_hours: float) -> float:
        if expected_hours <= 0:
            return 100.0
        ratio = hours_since / expected_hours
        if ratio <= 1.0:
            return 100.0
        # Exponential decay: score = 100 * e^(-k*(ratio-1)), k=0.7
        import math
        return max(0.0, 100.0 * math.exp(-0.7 * (ratio - 1.0)))

    def _status(self, hours_since: float, expected_hours: float) -> str:
        ratio = hours_since / max(expected_hours, 0.001)
        if ratio <= 1.0:
            return "fresh"
        elif ratio <= 1.5:
            return "slightly_stale"
        elif ratio <= 2.0:
            return "stale"
        return "very_stale"
