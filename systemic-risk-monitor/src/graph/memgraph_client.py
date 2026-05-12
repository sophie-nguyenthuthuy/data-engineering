"""
Memgraph client — manages the live interbank graph.

Schema
------
(:Institution {id, name, tier, balance, lending_capacity})
-[:TRANSFERS {tx_id, amount, tx_type, timestamp, weight}]->
(:Institution)

Aggregated edge (:Institution)-[:NET_EXPOSURE {total, count, last_ts}]->(:Institution)
is maintained as a materialized view updated on each ingest.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncSession

from src.config import settings
from src.generator.transaction_generator import Institution, Transaction

log = logging.getLogger(__name__)


class MemgraphClient:
    def __init__(self):
        uri = f"bolt://{settings.memgraph_host}:{settings.memgraph_port}"
        self._driver: AsyncDriver = AsyncGraphDatabase.driver(
            uri, auth=("", ""), encrypted=False
        )

    async def verify_connectivity(self, retries: int = 10, delay: float = 3.0) -> None:
        for attempt in range(retries):
            try:
                async with self._driver.session() as session:
                    await session.run("RETURN 1")
                log.info("Connected to Memgraph at %s:%s", settings.memgraph_host, settings.memgraph_port)
                return
            except Exception as exc:
                log.warning("Memgraph not ready (attempt %d/%d): %s", attempt + 1, retries, exc)
                await asyncio.sleep(delay)
        raise RuntimeError("Could not connect to Memgraph after retries")

    async def setup_schema(self) -> None:
        """Create indexes and constraints for the interbank graph."""
        async with self._driver.session() as session:
            await session.run(
                "CREATE INDEX ON :Institution(id);"
            )
            await session.run(
                "CREATE INDEX ON :Institution(tier);"
            )
        log.info("Schema indexes created")

    async def upsert_institution(self, inst: Institution) -> None:
        async with self._driver.session() as session:
            await session.run(
                """
                MERGE (i:Institution {id: $id})
                SET i.name = $name,
                    i.tier = $tier,
                    i.balance = $balance,
                    i.lending_capacity = $lending_capacity
                """,
                id=inst.id,
                name=inst.name,
                tier=inst.tier,
                balance=inst.balance,
                lending_capacity=inst.lending_capacity,
            )

    async def ingest_transaction(self, tx: Transaction) -> None:
        """Write transaction edge and update aggregated NET_EXPOSURE edge."""
        async with self._driver.session() as session:
            await session.run(
                """
                MATCH (s:Institution {id: $sender}), (r:Institution {id: $receiver})
                CREATE (s)-[:TRANSFERS {
                    tx_id:    $tx_id,
                    amount:   $amount,
                    tx_type:  $tx_type,
                    timestamp: $ts
                }]->(r)
                """,
                sender=tx.sender_id,
                receiver=tx.receiver_id,
                tx_id=tx.tx_id,
                amount=tx.amount,
                tx_type=tx.tx_type,
                ts=tx.timestamp,
            )
            # Upsert aggregated exposure edge
            await session.run(
                """
                MATCH (s:Institution {id: $sender}), (r:Institution {id: $receiver})
                MERGE (s)-[e:NET_EXPOSURE]->(r)
                ON CREATE SET e.total = $amount, e.count = 1, e.last_ts = $ts
                ON MATCH  SET e.total = e.total + $amount,
                              e.count = e.count + 1,
                              e.last_ts = $ts
                """,
                sender=tx.sender_id,
                receiver=tx.receiver_id,
                amount=tx.amount,
                ts=tx.timestamp,
            )

    async def get_all_nodes(self) -> list[dict]:
        async with self._driver.session() as session:
            result = await session.run(
                "MATCH (i:Institution) RETURN i.id AS id, i.name AS name, "
                "i.tier AS tier, i.balance AS balance, i.lending_capacity AS lc"
            )
            return [dict(r) async for r in result]

    async def get_all_edges(self) -> list[dict]:
        """Return aggregated NET_EXPOSURE edges."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (s:Institution)-[e:NET_EXPOSURE]->(r:Institution)
                RETURN s.id AS source, r.id AS target,
                       e.total AS total, e.count AS count, e.last_ts AS last_ts
                """
            )
            return [dict(r) async for r in result]

    async def get_recent_transactions(self, limit: int = 50) -> list[dict]:
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (s:Institution)-[t:TRANSFERS]->(r:Institution)
                RETURN s.id AS sender, r.id AS receiver,
                       t.tx_id AS tx_id, t.amount AS amount,
                       t.tx_type AS tx_type, t.timestamp AS ts
                ORDER BY t.timestamp DESC
                LIMIT $limit
                """,
                limit=limit,
            )
            return [dict(r) async for r in result]

    async def get_node_exposures(self, node_id: str) -> dict:
        """Total outbound and inbound for a single institution."""
        async with self._driver.session() as session:
            out_res = await session.run(
                """
                MATCH (s:Institution {id: $id})-[e:NET_EXPOSURE]->()
                RETURN coalesce(sum(e.total), 0) AS total_out
                """,
                id=node_id,
            )
            out_rec = await out_res.single()

            in_res = await session.run(
                """
                MATCH ()-[e:NET_EXPOSURE]->(r:Institution {id: $id})
                RETURN coalesce(sum(e.total), 0) AS total_in
                """,
                id=node_id,
            )
            in_rec = await in_res.single()

        return {
            "total_out": out_rec["total_out"] if out_rec else 0,
            "total_in": in_rec["total_in"] if in_rec else 0,
        }

    async def prune_old_transfers(self, max_age_seconds: int = 3600) -> int:
        """Remove raw TRANSFERS edges older than max_age_seconds to keep the graph lean."""
        import time
        cutoff = time.time() - max_age_seconds
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH ()-[t:TRANSFERS]->()
                WHERE t.timestamp < $cutoff
                DELETE t
                RETURN count(t) AS deleted
                """,
                cutoff=cutoff,
            )
            rec = await result.single()
            deleted = rec["deleted"] if rec else 0
        if deleted:
            log.info("Pruned %d stale TRANSFERS edges", deleted)
        return deleted

    async def close(self) -> None:
        await self._driver.close()
