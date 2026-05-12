\connect analytics

CREATE SCHEMA IF NOT EXISTS raw         AUTHORIZATION analytics;
CREATE SCHEMA IF NOT EXISTS bronze      AUTHORIZATION analytics;
CREATE SCHEMA IF NOT EXISTS silver      AUTHORIZATION analytics;
CREATE SCHEMA IF NOT EXISTS gold        AUTHORIZATION analytics;
CREATE SCHEMA IF NOT EXISTS snapshots   AUTHORIZATION analytics;

-- Create tables AS analytics so that role owns them (and default privileges
-- for analytics pick up future dbt-built tables in gold/silver).
SET ROLE analytics;

-- Raw landing tables. Airflow writes here; dbt reads them as sources.
CREATE TABLE IF NOT EXISTS raw.customers (
  id            BIGINT PRIMARY KEY,
  email         TEXT,
  full_name     TEXT,
  country       TEXT,
  created_at    TIMESTAMPTZ,
  updated_at    TIMESTAMPTZ,
  _ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  _payload      JSONB
);

CREATE TABLE IF NOT EXISTS raw.products (
  id            BIGINT PRIMARY KEY,
  sku           TEXT,
  name          TEXT,
  category      TEXT,
  price_cents   INTEGER,
  updated_at    TIMESTAMPTZ,
  _ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  _payload      JSONB
);

CREATE TABLE IF NOT EXISTS raw.orders (
  id            BIGINT PRIMARY KEY,
  customer_id   BIGINT,
  product_id    BIGINT,
  quantity      INTEGER,
  amount_cents  INTEGER,
  status        TEXT,
  ordered_at    TIMESTAMPTZ,
  updated_at    TIMESTAMPTZ,
  _ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  _payload      JSONB
);
CREATE INDEX IF NOT EXISTS orders_updated_at_idx ON raw.orders (updated_at);

-- Track incremental extraction watermarks per source.
CREATE TABLE IF NOT EXISTS raw._watermarks (
  source       TEXT PRIMARY KEY,
  last_updated TIMESTAMPTZ NOT NULL
);

RESET ROLE;

-- Read-only BI role. Grants apply to current + future tables in gold/silver.
GRANT USAGE ON SCHEMA gold, silver TO bi_read;
GRANT SELECT ON ALL TABLES IN SCHEMA gold, silver TO bi_read;
ALTER DEFAULT PRIVILEGES FOR ROLE analytics IN SCHEMA gold  GRANT SELECT ON TABLES TO bi_read;
ALTER DEFAULT PRIVILEGES FOR ROLE analytics IN SCHEMA silver GRANT SELECT ON TABLES TO bi_read;
