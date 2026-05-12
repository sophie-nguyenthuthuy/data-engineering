"""
PyFlink Table API job.

Source:  Kafka `trades` topic (JSON: ts_ms, symbol, price, volume, source)
Sinks:
  1. Kafka `trades.agg.5s` topic   (for the API WebSocket fan-out)
  2. JDBC   `trades` hypertable    (raw trade persistence)
  3. JDBC   `trades_agg_5s`        (aggregate persistence)

Aggregation: 5-second tumbling window per symbol — trade_count, VWAP,
min/max/avg price, total volume. Watermark tolerates 2s of out-of-orderness.
"""
from __future__ import annotations

import logging
import os
import time

from pyflink.table import EnvironmentSettings, TableEnvironment

log = logging.getLogger("flink-job")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def must(k: str) -> str:
    v = os.environ.get(k)
    if not v:
        raise SystemExit(f"missing env: {k}")
    return v


def build_env() -> TableEnvironment:
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    cfg = t_env.get_config().get_configuration()
    cfg.set_string("parallelism.default", "1")
    cfg.set_string("pipeline.name", "finnhub-aggregator")
    return t_env


def wait_for_kafka(bootstrap: str, attempts: int = 30, sleep_s: float = 2.0) -> None:
    """Best-effort sanity wait so the Flink mini-cluster doesn't race Kafka at boot."""
    import socket
    host, _, port = bootstrap.partition(":")
    port_i = int(port or "9092")
    for i in range(attempts):
        try:
            with socket.create_connection((host, port_i), timeout=2.0):
                log.info("kafka reachable at %s:%d", host, port_i)
                return
        except OSError:
            log.info("waiting for kafka %s:%d ... (%d/%d)", host, port_i, i + 1, attempts)
            time.sleep(sleep_s)
    raise SystemExit("kafka not reachable")


def main() -> None:
    bootstrap = must("KAFKA_BOOTSTRAP")
    trades_topic = must("TRADES_TOPIC")
    agg_topic = must("AGGREGATES_TOPIC")

    pg_host = must("POSTGRES_HOST")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")
    pg_db = must("POSTGRES_DB")
    pg_user = must("POSTGRES_USER")
    pg_pass = must("POSTGRES_PASSWORD")
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    wait_for_kafka(bootstrap)

    t_env = build_env()

    t_env.execute_sql(f"""
        CREATE TABLE trades_src (
            ts_ms   BIGINT,
            symbol  STRING,
            price   DOUBLE,
            volume  DOUBLE,
            source  STRING,
            event_time AS TO_TIMESTAMP_LTZ(ts_ms, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{trades_topic}',
            'properties.bootstrap.servers' = '{bootstrap}',
            'properties.group.id' = 'flink-aggregator',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Note: flink-connector-jdbc 3.1.x Postgres dialect does not support
    # TIMESTAMP_LTZ — we store as TIMESTAMP(3) (UTC, no TZ) and cast on insert.
    # Postgres accepts these into TIMESTAMPTZ columns as UTC wall-clock.
    t_env.execute_sql(f"""
        CREATE TABLE trades_jdbc (
            ts      TIMESTAMP(3),
            symbol  STRING,
            price   DOUBLE,
            volume  DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{jdbc_url}',
            'table-name' = 'trades',
            'username' = '{pg_user}',
            'password' = '{pg_pass}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '1s'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE agg_jdbc (
            window_start TIMESTAMP(3),
            window_end   TIMESTAMP(3),
            symbol       STRING,
            trade_count  BIGINT,
            avg_price    DOUBLE,
            min_price    DOUBLE,
            max_price    DOUBLE,
            total_volume DOUBLE,
            vwap         DOUBLE,
            PRIMARY KEY (symbol, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{jdbc_url}',
            'table-name' = 'trades_agg_5s',
            'username' = '{pg_user}',
            'password' = '{pg_pass}',
            'sink.buffer-flush.max-rows' = '200',
            'sink.buffer-flush.interval' = '500ms'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE agg_kafka (
            window_start TIMESTAMP_LTZ(3),
            window_end   TIMESTAMP_LTZ(3),
            symbol       STRING,
            trade_count  BIGINT,
            avg_price    DOUBLE,
            min_price    DOUBLE,
            max_price    DOUBLE,
            total_volume DOUBLE,
            vwap         DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{agg_topic}',
            'properties.bootstrap.servers' = '{bootstrap}',
            'format' = 'json',
            'sink.partitioner' = 'round-robin'
        )
    """)

    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql("""
        INSERT INTO trades_jdbc
        SELECT CAST(event_time AS TIMESTAMP(3)), symbol, price, volume FROM trades_src
    """)

    agg_core = """
        SELECT
            window_start,
            window_end,
            symbol,
            COUNT(*)                       AS trade_count,
            AVG(price)                     AS avg_price,
            MIN(price)                     AS min_price,
            MAX(price)                     AS max_price,
            SUM(volume)                    AS total_volume,
            CASE WHEN SUM(volume) > 0
                 THEN SUM(price * volume) / SUM(volume)
                 ELSE AVG(price)
            END                            AS vwap
        FROM TABLE(
            TUMBLE(TABLE trades_src, DESCRIPTOR(event_time), INTERVAL '5' SECONDS)
        )
        GROUP BY window_start, window_end, symbol
    """

    # Kafka sink keeps LTZ (JSON serializer supports it fine).
    stmt_set.add_insert_sql(f"INSERT INTO agg_kafka {agg_core}")

    # JDBC sink: cast timestamps down to TIMESTAMP(3).
    stmt_set.add_insert_sql(f"""
        INSERT INTO agg_jdbc
        SELECT
            CAST(window_start AS TIMESTAMP(3)),
            CAST(window_end   AS TIMESTAMP(3)),
            symbol, trade_count, avg_price, min_price, max_price, total_volume, vwap
        FROM ({agg_core}) t
    """)

    log.info("submitting statement set")
    job = stmt_set.execute()
    log.info("job submitted: %s", job.get_job_client().get_job_id() if job.get_job_client() else "no-client")
    # await_async_termination is the blocking form in PyFlink mini-cluster mode
    job.wait()


if __name__ == "__main__":
    main()
