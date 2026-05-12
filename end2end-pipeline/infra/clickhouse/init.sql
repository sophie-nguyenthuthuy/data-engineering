-- Initial ClickHouse schema for the pipeline.
-- Executed by clickhouse-server on first startup via /docker-entrypoint-initdb.d/.

CREATE DATABASE IF NOT EXISTS events;

-- ReplacingMergeTree(ingested_at) deduplicates rows that share the full
-- ORDER BY key. Since `event_id` is globally unique, any duplicate arriving
-- via at-least-once Kafka Connect retry will collapse on merge — the row
-- with the largest `ingested_at` wins.
--
-- Queries that require strict dedup-now semantics should use FINAL; the
-- minutely rollup (AggregatingMergeTree below) does NOT use FINAL, so a
-- small at-least-once drift is possible during active merges. Phase 4's
-- Dagster `OPTIMIZE TABLE ... FINAL` asset bounds the drift.
CREATE TABLE IF NOT EXISTS events.user_interactions
(
    event_id    String,
    occurred_at DateTime64(3, 'UTC'),
    user_id     String,
    session_id  String,
    event_type  LowCardinality(String),
    status      LowCardinality(String),
    error_code  Nullable(String),
    latency_ms  Int32,
    country     LowCardinality(String),
    device      LowCardinality(String),
    metadata    Map(String, String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toDate(occurred_at)
ORDER BY (event_type, occurred_at, event_id)
TTL toDateTime(occurred_at) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Rolling 1-minute aggregate for the dashboard.
-- Uses AggregatingMergeTree so latency distributions merge correctly.
CREATE TABLE IF NOT EXISTS events.user_interactions_1m
(
    minute            DateTime,
    event_type        LowCardinality(String),
    status            LowCardinality(String),
    events            AggregateFunction(count, UInt64),
    latency_sum       AggregateFunction(sum, Int64),
    latency_p95_state AggregateFunction(quantileTDigest(0.95), Int32)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (minute, event_type, status);

CREATE MATERIALIZED VIEW IF NOT EXISTS events.user_interactions_1m_mv
TO events.user_interactions_1m
AS
SELECT
    toStartOfMinute(occurred_at)           AS minute,
    event_type,
    status,
    countState()                           AS events,
    sumState(toInt64(latency_ms))          AS latency_sum,
    quantileTDigestState(0.95)(latency_ms) AS latency_p95_state
FROM events.user_interactions
GROUP BY minute, event_type, status;

-- Phase 4: Dagster writes hourly Spark aggregates here.
-- ReplacingMergeTree(generated_at) so re-runs of the same partition collapse
-- on merge — safe for retries without a separate DELETE.
CREATE TABLE IF NOT EXISTS events.analysis_hourly
(
    window_start   DateTime('UTC'),
    event_type     LowCardinality(String),
    status         LowCardinality(String),
    country        LowCardinality(String),
    device         LowCardinality(String),
    events         UInt64,
    errors         UInt64,
    p95_latency_ms Float64,
    avg_latency_ms Float64,
    generated_at   DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(generated_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, event_type, status, country, device)
TTL window_start + INTERVAL 90 DAY;
