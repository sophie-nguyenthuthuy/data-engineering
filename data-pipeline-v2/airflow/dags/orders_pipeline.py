"""Orders pipeline: mock-API -> raw -> dbt (bronze/silver/gold).

Extraction is incremental via a per-source watermark in raw._watermarks.
dbt is expanded into one Airflow task per model via Cosmos so failures are
visible at model granularity instead of inside a single `dbt run` blob.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable

import httpx
import pendulum
import psycopg2
import psycopg2.extras
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

log = logging.getLogger(__name__)

MOCK_API_URL = os.environ["MOCK_API_URL"]
PAGE_SIZE = 500
HTTP_TIMEOUT = 30.0

SOURCES = {
    "customers": {
        "endpoint": "/customers",
        "table": "raw.customers",
        "columns": ("id", "email", "full_name", "country", "created_at", "updated_at"),
    },
    "products": {
        "endpoint": "/products",
        "table": "raw.products",
        "columns": ("id", "sku", "name", "category", "price_cents", "updated_at"),
    },
    "orders": {
        "endpoint": "/orders",
        "table": "raw.orders",
        "columns": (
            "id", "customer_id", "product_id", "quantity",
            "amount_cents", "status", "ordered_at", "updated_at",
        ),
    },
}


def _pg_conn():
    return psycopg2.connect(os.environ["ANALYTICS_DB_URL"])


def _get_watermark(conn, source: str) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT last_updated FROM raw._watermarks WHERE source = %s", (source,))
        row = cur.fetchone()
    if row:
        return row[0].astimezone(timezone.utc).isoformat()
    # Default: 1970 — first run pulls everything.
    return "1970-01-01T00:00:00+00:00"


def _set_watermark(conn, source: str, ts: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw._watermarks (source, last_updated)
            VALUES (%s, %s)
            ON CONFLICT (source) DO UPDATE SET last_updated = EXCLUDED.last_updated
            """,
            (source, ts),
        )


def _fetch_all(endpoint: str, updated_since: str) -> Iterable[dict]:
    offset = 0
    with httpx.Client(base_url=MOCK_API_URL, timeout=HTTP_TIMEOUT) as client:
        while True:
            resp = client.get(
                endpoint,
                params={"limit": PAGE_SIZE, "offset": offset, "updated_since": updated_since},
            )
            resp.raise_for_status()
            body = resp.json()
            items = body.get("items", [])
            if not items:
                return
            yield from items
            offset += len(items)
            if offset >= body["total"]:
                return


def _upsert(conn, table: str, columns: tuple[str, ...], rows: list[dict]) -> int:
    if not rows:
        return 0
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * (len(columns) + 1))  # +1 for _payload
    updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c != "id"])
    sql = (
        f"INSERT INTO {table} ({col_list}, _payload) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {updates}, _ingested_at = now(), _payload = EXCLUDED._payload"
    )
    values = [
        tuple(r.get(c) for c in columns) + (json.dumps(r),)
        for r in rows
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, values, page_size=500)
    return len(rows)


@dag(
    dag_id="orders_pipeline",
    description="Mock API -> raw -> dbt (bronze/silver/gold)",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["analytics", "orders"],
)
def orders_pipeline():
    @task
    def extract(source: str) -> dict:
        cfg = SOURCES[source]
        rows_loaded = 0
        max_updated = None
        with _pg_conn() as conn:
            watermark = _get_watermark(conn, source)
            log.info("source=%s watermark=%s", source, watermark)
            batch: list[dict] = []
            for row in _fetch_all(cfg["endpoint"], watermark):
                batch.append(row)
                if row["updated_at"] > (max_updated or ""):
                    max_updated = row["updated_at"]
                if len(batch) >= 1000:
                    rows_loaded += _upsert(conn, cfg["table"], cfg["columns"], batch)
                    batch.clear()
            rows_loaded += _upsert(conn, cfg["table"], cfg["columns"], batch)
            if max_updated:
                _set_watermark(conn, source, max_updated)
            conn.commit()
        log.info("source=%s rows_loaded=%d max_updated=%s", source, rows_loaded, max_updated)
        return {"source": source, "rows": rows_loaded, "max_updated": max_updated}

    @task
    def assert_freshness(results: list[dict]) -> None:
        """Fail the DAG if any source returned zero rows on the very first run."""
        for r in results:
            if r["rows"] == 0 and r["max_updated"] is None:
                raise ValueError(f"source {r['source']} returned no rows and has no prior watermark")

    extracted = extract.expand(source=list(SOURCES.keys()))
    check = assert_freshness(extracted)

    dbt_project_dir = "/opt/airflow/dbt"
    profile_config = ProfileConfig(
        profile_name="analytics",
        target_name="prod",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="postgres_analytics",
            profile_args={"schema": "bronze"},
        ),
    )
    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(dbt_project_dir),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/home/airflow/.local/bin/dbt"),
        default_args={"retries": 1},
    )

    chain(check, dbt_build)


orders_pipeline()
