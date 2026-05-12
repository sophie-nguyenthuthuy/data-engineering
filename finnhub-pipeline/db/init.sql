CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS trades (
    ts          TIMESTAMPTZ NOT NULL,
    symbol      TEXT        NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    volume      DOUBLE PRECISION NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('trades', 'ts', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 hour');
CREATE INDEX IF NOT EXISTS trades_symbol_ts_idx ON trades (symbol, ts DESC);

CREATE TABLE IF NOT EXISTS trades_agg_5s (
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    symbol       TEXT        NOT NULL,
    trade_count  BIGINT      NOT NULL,
    avg_price    DOUBLE PRECISION NOT NULL,
    min_price    DOUBLE PRECISION NOT NULL,
    max_price    DOUBLE PRECISION NOT NULL,
    total_volume DOUBLE PRECISION NOT NULL,
    vwap         DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (symbol, window_start)
);

SELECT create_hypertable('trades_agg_5s', 'window_start', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');
CREATE INDEX IF NOT EXISTS trades_agg_5s_symbol_idx ON trades_agg_5s (symbol, window_start DESC);

ALTER TABLE trades SET (timescaledb.compress, timescaledb.compress_segmentby = 'symbol');
SELECT add_compression_policy('trades', INTERVAL '1 day', if_not_exists => TRUE);

SELECT add_retention_policy('trades', INTERVAL '7 days', if_not_exists => TRUE);
