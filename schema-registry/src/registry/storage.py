from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite

from .models import CompatibilityMode, MigrationScript, MigrationStep, SchemaType, SchemaVersion, SubjectConfig


DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS subjects (
    name TEXT PRIMARY KEY,
    compatibility TEXT NOT NULL DEFAULT 'BACKWARD',
    normalize INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS schema_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject TEXT NOT NULL,
    version INTEGER NOT NULL,
    schema_type TEXT NOT NULL DEFAULT 'JSON',
    schema_definition TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    UNIQUE(subject, version),
    FOREIGN KEY(subject) REFERENCES subjects(name)
);

CREATE TABLE IF NOT EXISTS migration_scripts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject TEXT NOT NULL,
    from_version INTEGER NOT NULL,
    to_version INTEGER NOT NULL,
    steps TEXT NOT NULL,
    dsl_source TEXT NOT NULL DEFAULT '',
    auto_generated INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    breaking_changes TEXT NOT NULL DEFAULT '[]',
    UNIQUE(subject, from_version, to_version)
);
"""


class Storage:
    def __init__(self, db_path: str = "registry.db"):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(DB_SCHEMA)
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    @property
    def db(self) -> aiosqlite.Connection:
        if not self._db:
            raise RuntimeError("Storage not connected. Call connect() first.")
        return self._db

    # ── Subjects ────────────────────────────────────────────────────────────

    async def get_subjects(self) -> list[str]:
        async with self.db.execute("SELECT name FROM subjects ORDER BY name") as cur:
            rows = await cur.fetchall()
        return [r["name"] for r in rows]

    async def get_subject_config(self, subject: str) -> SubjectConfig | None:
        async with self.db.execute(
            "SELECT name, compatibility, normalize FROM subjects WHERE name = ?", (subject,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        return SubjectConfig(
            subject=row["name"],
            compatibility=CompatibilityMode(row["compatibility"]),
            normalize=bool(row["normalize"]),
        )

    async def upsert_subject_config(self, config: SubjectConfig) -> None:
        await self.db.execute(
            """INSERT INTO subjects(name, compatibility, normalize)
               VALUES(?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                 compatibility=excluded.compatibility,
                 normalize=excluded.normalize""",
            (config.subject, config.compatibility.value, int(config.normalize)),
        )
        await self.db.commit()

    async def delete_subject(self, subject: str) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) as cnt FROM schema_versions WHERE subject = ?", (subject,)
        ) as cur:
            row = await cur.fetchone()
        count = row["cnt"] if row else 0
        await self.db.execute("DELETE FROM schema_versions WHERE subject = ?", (subject,))
        await self.db.execute("DELETE FROM migration_scripts WHERE subject = ?", (subject,))
        await self.db.execute("DELETE FROM subjects WHERE name = ?", (subject,))
        await self.db.commit()
        return count

    # ── Schema Versions ──────────────────────────────────────────────────────

    async def get_versions(self, subject: str) -> list[int]:
        async with self.db.execute(
            "SELECT version FROM schema_versions WHERE subject = ? ORDER BY version", (subject,)
        ) as cur:
            rows = await cur.fetchall()
        return [r["version"] for r in rows]

    async def get_schema_version(self, subject: str, version: int) -> SchemaVersion | None:
        async with self.db.execute(
            "SELECT * FROM schema_versions WHERE subject = ? AND version = ?", (subject, version)
        ) as cur:
            row = await cur.fetchone()
        return self._row_to_schema_version(row) if row else None

    async def get_latest_schema_version(self, subject: str) -> SchemaVersion | None:
        async with self.db.execute(
            "SELECT * FROM schema_versions WHERE subject = ? ORDER BY version DESC LIMIT 1",
            (subject,),
        ) as cur:
            row = await cur.fetchone()
        return self._row_to_schema_version(row) if row else None

    async def get_all_schema_versions(self, subject: str) -> list[SchemaVersion]:
        async with self.db.execute(
            "SELECT * FROM schema_versions WHERE subject = ? ORDER BY version", (subject,)
        ) as cur:
            rows = await cur.fetchall()
        return [self._row_to_schema_version(r) for r in rows]

    async def save_schema_version(self, sv: SchemaVersion) -> SchemaVersion:
        # Ensure subject row exists
        await self.db.execute(
            "INSERT OR IGNORE INTO subjects(name, compatibility) VALUES(?, ?)",
            (sv.subject, CompatibilityMode.BACKWARD.value),
        )
        cursor = await self.db.execute(
            """INSERT INTO schema_versions
                 (subject, version, schema_type, schema_definition, schema_hash, created_at, metadata)
               VALUES(?, ?, ?, ?, ?, ?, ?)""",
            (
                sv.subject,
                sv.version,
                sv.schema_type.value,
                json.dumps(sv.schema_definition),
                sv.schema_hash,
                sv.created_at.isoformat(),
                json.dumps(sv.metadata),
            ),
        )
        await self.db.commit()
        sv.id = cursor.lastrowid
        return sv

    async def delete_version(self, subject: str, version: int) -> bool:
        cur = await self.db.execute(
            "DELETE FROM schema_versions WHERE subject = ? AND version = ?", (subject, version)
        )
        await self.db.commit()
        return cur.rowcount > 0

    def _row_to_schema_version(self, row: aiosqlite.Row) -> SchemaVersion:
        return SchemaVersion(
            id=row["id"],
            subject=row["subject"],
            version=row["version"],
            schema_type=SchemaType(row["schema_type"]),
            schema_definition=json.loads(row["schema_definition"]),
            schema_hash=row["schema_hash"],
            created_at=datetime.fromisoformat(row["created_at"]),
            metadata=json.loads(row["metadata"]),
        )

    # ── Migration Scripts ────────────────────────────────────────────────────

    async def save_migration(self, script: MigrationScript) -> MigrationScript:
        cursor = await self.db.execute(
            """INSERT INTO migration_scripts
                 (subject, from_version, to_version, steps, dsl_source, auto_generated, created_at, breaking_changes)
               VALUES(?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(subject, from_version, to_version) DO UPDATE SET
                 steps=excluded.steps,
                 dsl_source=excluded.dsl_source,
                 auto_generated=excluded.auto_generated,
                 breaking_changes=excluded.breaking_changes""",
            (
                script.subject,
                script.from_version,
                script.to_version,
                json.dumps([s.model_dump() for s in script.steps]),
                script.dsl_source,
                int(script.auto_generated),
                script.created_at.isoformat(),
                json.dumps(script.breaking_changes),
            ),
        )
        await self.db.commit()
        script.id = cursor.lastrowid
        return script

    async def get_migration(self, subject: str, from_version: int, to_version: int) -> MigrationScript | None:
        async with self.db.execute(
            "SELECT * FROM migration_scripts WHERE subject=? AND from_version=? AND to_version=?",
            (subject, from_version, to_version),
        ) as cur:
            row = await cur.fetchone()
        return self._row_to_migration(row) if row else None

    async def get_migrations_for_subject(self, subject: str) -> list[MigrationScript]:
        async with self.db.execute(
            "SELECT * FROM migration_scripts WHERE subject=? ORDER BY from_version, to_version",
            (subject,),
        ) as cur:
            rows = await cur.fetchall()
        return [self._row_to_migration(r) for r in rows]

    def _row_to_migration(self, row: aiosqlite.Row) -> MigrationScript:
        raw_steps = json.loads(row["steps"])
        return MigrationScript(
            id=row["id"],
            subject=row["subject"],
            from_version=row["from_version"],
            to_version=row["to_version"],
            steps=[MigrationStep(**s) for s in raw_steps],
            dsl_source=row["dsl_source"],
            auto_generated=bool(row["auto_generated"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            breaking_changes=json.loads(row["breaking_changes"]),
        )
