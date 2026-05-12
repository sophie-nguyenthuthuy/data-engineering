"""
Metadata registry — register tables and columns with their descriptions,
owners, domains, and tags.
"""

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ColumnMeta:
    column_name: str
    data_type: str
    description: str = ""
    is_pii: bool = False
    is_nullable: bool = True
    sample_values: list = field(default_factory=list)


@dataclass
class TableMeta:
    table_name: str
    description: str
    owner: str
    domain: str
    source_system: str = ""
    update_frequency: str = "daily"
    tags: list = field(default_factory=list)
    columns: list[ColumnMeta] = field(default_factory=list)


class MetadataRegistry:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def register_table(self, meta: TableMeta) -> None:
        now = _now()
        self.conn.execute(
            """
            INSERT INTO meta_tables
                (table_name, description, owner, domain, source_system,
                 update_frequency, tags, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(table_name) DO UPDATE SET
                description      = excluded.description,
                owner            = excluded.owner,
                domain           = excluded.domain,
                source_system    = excluded.source_system,
                update_frequency = excluded.update_frequency,
                tags             = excluded.tags,
                updated_at       = excluded.updated_at
            """,
            (
                meta.table_name,
                meta.description,
                meta.owner,
                meta.domain,
                meta.source_system,
                meta.update_frequency,
                json.dumps(meta.tags),
                now,
                now,
            ),
        )
        for col in meta.columns:
            self.conn.execute(
                """
                INSERT INTO meta_columns
                    (table_name, column_name, data_type, description,
                     is_pii, is_nullable, sample_values)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(table_name, column_name) DO UPDATE SET
                    data_type     = excluded.data_type,
                    description   = excluded.description,
                    is_pii        = excluded.is_pii,
                    is_nullable   = excluded.is_nullable,
                    sample_values = excluded.sample_values
                """,
                (
                    meta.table_name,
                    col.column_name,
                    col.data_type,
                    col.description,
                    int(col.is_pii),
                    int(col.is_nullable),
                    json.dumps(col.sample_values),
                ),
            )
        self.conn.commit()

    def deprecate_table(self, table_name: str, note: str) -> None:
        self.conn.execute(
            "UPDATE meta_tables SET is_deprecated=1, deprecation_note=? WHERE table_name=?",
            (note, table_name),
        )
        self.conn.commit()

    def get_table(self, table_name: str) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM meta_tables WHERE table_name=?", (table_name,)
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["tags"] = json.loads(result["tags"] or "[]")
        result["columns"] = self._get_columns(table_name)
        return result

    def _get_columns(self, table_name: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM meta_columns WHERE table_name=? ORDER BY id",
            (table_name,),
        ).fetchall()
        cols = []
        for r in rows:
            d = dict(r)
            d["sample_values"] = json.loads(d["sample_values"] or "[]")
            cols.append(d)
        return cols

    def list_tables(self, domain: Optional[str] = None, include_deprecated: bool = False) -> list[dict]:
        query = "SELECT * FROM meta_tables WHERE 1=1"
        params: list = []
        if domain:
            query += " AND domain=?"
            params.append(domain)
        if not include_deprecated:
            query += " AND is_deprecated=0"
        rows = self.conn.execute(query, params).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["tags"] = json.loads(d["tags"] or "[]")
            result.append(d)
        return result
