"""
SQLite-backed regional store.
Every region has its own fully independent DB — no shared storage.
Replication is handled at the application layer.
"""
import json
import time
import aiosqlite
from typing import Optional

from src.models import AccountRecord, VectorClock, ConflictEvent


class RegionalStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def open(self):
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._create_tables()

    async def close(self):
        if self._db:
            await self._db.close()

    async def _create_tables(self):
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS accounts (
                account_id        TEXT PRIMARY KEY,
                owner             TEXT NOT NULL,
                balance           REAL NOT NULL DEFAULT 0,
                currency          TEXT NOT NULL DEFAULT 'USD',
                tags              TEXT NOT NULL DEFAULT '[]',
                metadata          TEXT NOT NULL DEFAULT '{}',
                vector_clock      TEXT NOT NULL DEFAULT '{}',
                wall_time         REAL NOT NULL,
                origin_region     TEXT NOT NULL DEFAULT '',
                last_writer_region TEXT NOT NULL DEFAULT '',
                crdt_credits      TEXT NOT NULL DEFAULT '{}',
                crdt_debits       TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS conflict_log (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id        TEXT NOT NULL,
                strategy_used     TEXT NOT NULL,
                local_wall_time   REAL NOT NULL,
                remote_wall_time  REAL NOT NULL,
                local_region      TEXT NOT NULL,
                remote_region     TEXT NOT NULL,
                resolution        TEXT NOT NULL,
                resolved_at       REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS replication_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                direction   TEXT NOT NULL,   -- 'in' | 'out'
                peer_url    TEXT NOT NULL,
                record_count INTEGER NOT NULL,
                lag_seconds  REAL,
                logged_at   REAL NOT NULL
            );
        """)
        await self._db.commit()

    # ------------------------------------------------------------------ #
    #  Account CRUD                                                        #
    # ------------------------------------------------------------------ #

    def _row_to_record(self, row) -> AccountRecord:
        return AccountRecord(
            account_id=row["account_id"],
            owner=row["owner"],
            balance=row["balance"],
            currency=row["currency"],
            tags=json.loads(row["tags"]),
            metadata=json.loads(row["metadata"]),
            vector_clock=VectorClock(clocks=json.loads(row["vector_clock"])),
            wall_time=row["wall_time"],
            origin_region=row["origin_region"],
            last_writer_region=row["last_writer_region"],
            crdt_credits=json.loads(row["crdt_credits"]),
            crdt_debits=json.loads(row["crdt_debits"]),
        )

    async def get_account(self, account_id: str) -> Optional[AccountRecord]:
        async with self._db.execute(
            "SELECT * FROM accounts WHERE account_id = ?", (account_id,)
        ) as cur:
            row = await cur.fetchone()
        return self._row_to_record(row) if row else None

    async def list_accounts(self) -> list[AccountRecord]:
        async with self._db.execute("SELECT * FROM accounts") as cur:
            rows = await cur.fetchall()
        return [self._row_to_record(r) for r in rows]

    async def upsert_account(self, rec: AccountRecord) -> None:
        await self._db.execute("""
            INSERT INTO accounts
                (account_id, owner, balance, currency, tags, metadata,
                 vector_clock, wall_time, origin_region, last_writer_region,
                 crdt_credits, crdt_debits)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(account_id) DO UPDATE SET
                owner=excluded.owner,
                balance=excluded.balance,
                currency=excluded.currency,
                tags=excluded.tags,
                metadata=excluded.metadata,
                vector_clock=excluded.vector_clock,
                wall_time=excluded.wall_time,
                origin_region=excluded.origin_region,
                last_writer_region=excluded.last_writer_region,
                crdt_credits=excluded.crdt_credits,
                crdt_debits=excluded.crdt_debits
        """, (
            rec.account_id, rec.owner, rec.balance, rec.currency,
            json.dumps(rec.tags), json.dumps(rec.metadata),
            json.dumps(rec.vector_clock.clocks), rec.wall_time,
            rec.origin_region, rec.last_writer_region,
            json.dumps(rec.crdt_credits), json.dumps(rec.crdt_debits),
        ))
        await self._db.commit()

    async def count_accounts(self) -> int:
        async with self._db.execute("SELECT COUNT(*) FROM accounts") as cur:
            row = await cur.fetchone()
        return row[0]

    # ------------------------------------------------------------------ #
    #  Conflict log                                                        #
    # ------------------------------------------------------------------ #

    async def log_conflict(self, evt: ConflictEvent) -> None:
        await self._db.execute("""
            INSERT INTO conflict_log
                (account_id, strategy_used, local_wall_time, remote_wall_time,
                 local_region, remote_region, resolution, resolved_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            evt.account_id, evt.strategy_used, evt.local_wall_time,
            evt.remote_wall_time, evt.local_region, evt.remote_region,
            evt.resolution, evt.resolved_at,
        ))
        await self._db.commit()

    async def count_conflicts(self) -> int:
        async with self._db.execute("SELECT COUNT(*) FROM conflict_log") as cur:
            row = await cur.fetchone()
        return row[0]

    async def recent_conflicts(self, limit: int = 10) -> list[ConflictEvent]:
        async with self._db.execute(
            "SELECT * FROM conflict_log ORDER BY resolved_at DESC LIMIT ?", (limit,)
        ) as cur:
            rows = await cur.fetchall()
        return [
            ConflictEvent(
                account_id=r["account_id"],
                strategy_used=r["strategy_used"],
                local_wall_time=r["local_wall_time"],
                remote_wall_time=r["remote_wall_time"],
                local_region=r["local_region"],
                remote_region=r["remote_region"],
                resolution=r["resolution"],
                resolved_at=r["resolved_at"],
            ) for r in rows
        ]

    # ------------------------------------------------------------------ #
    #  Replication log                                                     #
    # ------------------------------------------------------------------ #

    async def log_replication(
        self, direction: str, peer_url: str, record_count: int,
        lag_seconds: Optional[float] = None
    ) -> None:
        await self._db.execute("""
            INSERT INTO replication_log (direction, peer_url, record_count, lag_seconds, logged_at)
            VALUES (?,?,?,?,?)
        """, (direction, peer_url, record_count, lag_seconds, time.time()))
        await self._db.commit()

    async def count_replicated(self, direction: str) -> int:
        async with self._db.execute(
            "SELECT COALESCE(SUM(record_count),0) FROM replication_log WHERE direction=?",
            (direction,)
        ) as cur:
            row = await cur.fetchone()
        return row[0]

    async def latest_lag(self) -> Optional[float]:
        async with self._db.execute(
            "SELECT lag_seconds FROM replication_log WHERE direction='in' "
            "ORDER BY logged_at DESC LIMIT 1"
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else None

    async def get_all_records_since(self, since: float) -> list[AccountRecord]:
        """Return accounts modified after `since` (wall_time)."""
        async with self._db.execute(
            "SELECT * FROM accounts WHERE wall_time > ?", (since,)
        ) as cur:
            rows = await cur.fetchall()
        return [self._row_to_record(r) for r in rows]
