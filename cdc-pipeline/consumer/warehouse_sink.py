"""
Warehouse sink — idempotent upserts to the data warehouse PostgreSQL.

All writes use INSERT ... ON CONFLICT DO UPDATE so that:
  - Duplicate delivery (Kafka at-least-once) is safe
  - Out-of-order events that arrive after a newer version are skipped
    (only update when incoming LSN > stored LSN)
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# Maps Kafka topic suffix to (warehouse_table, primary_key_column)
TOPIC_TABLE_MAP = {
    "cdc.public.users":       ("dim_users",       "id"),
    "cdc.public.orders":      ("fact_orders",      "id"),
    "cdc.public.order_items": ("fact_order_items", "id"),
}


class WarehouseSink:
    def __init__(self, dsn: str):
        self._dsn  = dsn
        self._conn: Optional[psycopg2.extensions.connection] = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> None:
        self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = False
        log.info("Connected to warehouse")

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def _cursor(self):
        if not self._conn or self._conn.closed:
            self.connect()
        return self._conn.cursor()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def apply_batch(self, events: List[dict]) -> int:
        """Apply a batch of enriched CDC events. Returns count of events applied."""
        applied = 0
        with self._conn:                 # transaction per batch
            cur = self._cursor()
            for event in events:
                try:
                    self._apply_one(cur, event)
                    applied += 1
                except Exception as exc:
                    log.error("Failed to apply event, routing to DLQ: %s", exc)
                    self._dead_letter(cur, event, str(exc))
        return applied

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _apply_one(self, cur, event: dict) -> None:
        meta    = event.get("_meta", {})
        topic   = meta.get("topic", "")
        lsn     = meta.get("lsn", 0)
        ts_ms   = meta.get("ts_ms", 0)
        op      = event.get("__op", "r")          # c=create r=read u=update d=delete
        deleted = event.get("__deleted", "false")

        table_info = TOPIC_TABLE_MAP.get(topic)
        if not table_info:
            log.debug("No mapping for topic %s — skipping", topic)
            return

        table, pk_col = table_info
        record_id = event.get("id")

        # Audit log every event regardless of outcome
        self._write_audit(cur, meta, table, record_id, op, lsn, ts_ms, event)

        if deleted == "true" or op == "d":
            self._handle_delete(cur, table, pk_col, record_id, lsn)
        else:
            self._handle_upsert(cur, table, pk_col, event, lsn, ts_ms, op)

    def _handle_upsert(self, cur, table: str, pk_col: str, event: dict, lsn: int, ts_ms: int, op: str) -> None:
        row = self._build_row(table, event, lsn, ts_ms, op)
        if not row:
            return

        columns   = list(row.keys())
        values    = [row[c] for c in columns]
        col_list  = ", ".join(f'"{c}"' for c in columns)
        val_list  = ", ".join("%s" for _ in columns)
        # Skip update if stored LSN is already newer (guards against out-of-order late arrivals)
        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"'
            for c in columns
            if c not in (pk_col, "_cdc_lsn", "_dw_loaded_at")
        )

        sql = f"""
            INSERT INTO {table} ({col_list})
            VALUES ({val_list})
            ON CONFLICT ("{pk_col}") DO UPDATE
              SET {update_set},
                  "_cdc_lsn" = EXCLUDED."_cdc_lsn",
                  "_dw_loaded_at" = NOW()
            WHERE {table}."_cdc_lsn" IS NULL
               OR {table}."_cdc_lsn" <= EXCLUDED."_cdc_lsn"
        """
        cur.execute(sql, values)

    def _handle_delete(self, cur, table: str, pk_col: str, record_id: Any, lsn: int) -> None:
        # Soft-delete: mark _cdc_op = 'd' rather than physical delete
        cur.execute(
            f'UPDATE {table} SET "_cdc_op" = %s, "_dw_loaded_at" = NOW() '
            f'WHERE "{pk_col}" = %s AND ("_cdc_lsn" IS NULL OR "_cdc_lsn" <= %s)',
            ("d", record_id, lsn),
        )

    def _build_row(self, table: str, event: dict, lsn: int, ts_ms: int, op: str) -> Optional[Dict]:
        """Map a CDC event payload to the warehouse row columns."""
        common = {
            "_cdc_op":       (op or "r")[0],
            "_cdc_lsn":      lsn,
            "_cdc_ts":       _ms_to_dt(ts_ms),
            "_cdc_tx_id":    event.get("__txId"),
            "_schema_version": event.get("_schema_version"),
        }

        if table == "dim_users":
            return {
                "id":             event.get("id"),
                "email":          event.get("email"),
                "username":       event.get("username"),
                "account_status": event.get("account_status") or event.get("status"),
                "tier":           event.get("tier"),
                "phone":          event.get("phone"),
                "created_at":     _parse_ts(event.get("created_at")),
                "updated_at":     _parse_ts(event.get("updated_at")),
                **common,
            }
        if table == "fact_orders":
            return {
                "id":           event.get("id"),
                "user_id":      event.get("user_id"),
                "status":       event.get("status"),
                "total_amount": event.get("total_amount"),
                "currency":     event.get("currency"),
                "created_at":   _parse_ts(event.get("created_at")),
                "updated_at":   _parse_ts(event.get("updated_at")),
                **common,
            }
        if table == "fact_order_items":
            return {
                "id":         event.get("id"),
                "order_id":   event.get("order_id"),
                "sku":        event.get("sku"),
                "quantity":   event.get("quantity"),
                "unit_price": event.get("unit_price"),
                "created_at": _parse_ts(event.get("created_at")),
                **common,
            }
        return None

    def _write_audit(self, cur, meta: dict, table: str, record_id, op: str, lsn: int, ts_ms: int, event: dict) -> None:
        cur.execute(
            """INSERT INTO cdc_audit_log
               (topic, partition, kafka_offset, table_name, record_id, op, lsn, event_ts, payload)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                meta.get("topic"), meta.get("partition"), meta.get("offset"),
                table, record_id, (op or "r")[0], lsn,
                _ms_to_dt(ts_ms),
                psycopg2.extras.Json({k: v for k, v in event.items() if not k.startswith("_meta")}),
            ),
        )

    def _dead_letter(self, cur, event: dict, error: str) -> None:
        meta = event.get("_meta", {})
        cur.execute(
            """INSERT INTO cdc_dead_letter
               (topic, partition, kafka_offset, error_message, raw_payload)
               VALUES (%s, %s, %s, %s, %s)""",
            (
                meta.get("topic"), meta.get("partition"), meta.get("offset"),
                error,
                psycopg2.extras.Json(event),
            ),
        )

    def update_watermark(self, topic: str, partition: int, lsn: int, offset: int) -> None:
        with self._conn:
            cur = self._cursor()
            cur.execute(
                """INSERT INTO cdc_watermarks (topic, partition, last_lsn, last_offset, updated_at)
                   VALUES (%s, %s, %s, %s, NOW())
                   ON CONFLICT (topic, partition) DO UPDATE
                     SET last_lsn    = GREATEST(cdc_watermarks.last_lsn, EXCLUDED.last_lsn),
                         last_offset = GREATEST(cdc_watermarks.last_offset, EXCLUDED.last_offset),
                         updated_at  = NOW()""",
                (topic, partition, lsn, offset),
            )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _ms_to_dt(ts_ms) -> Optional[datetime]:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)


def _parse_ts(value) -> Optional[datetime]:
    """Accept epoch-ms int, ISO string, or None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return _ms_to_dt(value)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None
