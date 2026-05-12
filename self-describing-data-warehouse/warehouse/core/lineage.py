"""
Lineage tracker — directed graph of table dependencies.
Supports upstream/downstream traversal and impact analysis.
"""

from datetime import datetime, timezone
import sqlite3
from collections import deque


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class LineageTracker:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def add_edge(self, upstream: str, downstream: str, transformation: str = "") -> None:
        self.conn.execute(
            """
            INSERT INTO meta_lineage (upstream_table, downstream_table, transformation, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(upstream_table, downstream_table) DO UPDATE SET
                transformation = excluded.transformation
            """,
            (upstream, downstream, transformation, _now()),
        )
        self.conn.commit()

    def upstream(self, table_name: str, depth: int = 10) -> list[dict]:
        """Return all tables that feed into this table (BFS)."""
        return self._bfs(table_name, direction="upstream", max_depth=depth)

    def downstream(self, table_name: str, depth: int = 10) -> list[dict]:
        """Return all tables that depend on this table (BFS)."""
        return self._bfs(table_name, direction="downstream", max_depth=depth)

    def _bfs(self, start: str, direction: str, max_depth: int) -> list[dict]:
        visited = {}
        queue = deque([(start, 0)])
        while queue:
            node, level = queue.popleft()
            if node in visited or level >= max_depth:
                continue
            if node != start:
                visited[node] = level
            if direction == "upstream":
                rows = self.conn.execute(
                    "SELECT upstream_table as neighbor, transformation FROM meta_lineage WHERE downstream_table=?",
                    (node,),
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT downstream_table as neighbor, transformation FROM meta_lineage WHERE upstream_table=?",
                    (node,),
                ).fetchall()
            for row in rows:
                neighbor = row["neighbor"]
                if neighbor not in visited:
                    queue.append((neighbor, level + 1))
        results = []
        for table, depth_val in visited.items():
            results.append({"table_name": table, "depth": depth_val})
        results.sort(key=lambda x: x["depth"])
        return results

    def impact_analysis(self, table_name: str) -> dict:
        """What breaks if this table goes down?"""
        affected = self.downstream(table_name)
        return {
            "table": table_name,
            "affected_tables": affected,
            "total_affected": len(affected),
        }

    def full_graph(self) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM meta_lineage").fetchall()
        return [dict(r) for r in rows]
