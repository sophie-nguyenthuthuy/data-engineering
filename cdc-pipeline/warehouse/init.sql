-- Data warehouse schema — denormalized, append-friendly
-- No FK constraints; CDC consumer handles upserts idempotently

CREATE TABLE dim_users (
    id              INT          NOT NULL,
    email           VARCHAR(255),
    username        VARCHAR(100),
    account_status  VARCHAR(50),
    tier            VARCHAR(50),
    phone           VARCHAR(50),
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    -- CDC metadata
    _cdc_op         CHAR(1),        -- c=create r=read u=update d=delete
    _cdc_lsn        BIGINT,
    _cdc_ts         TIMESTAMPTZ,
    _cdc_tx_id      BIGINT,
    _schema_version INT,
    _dw_loaded_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE TABLE fact_orders (
    id              INT          NOT NULL,
    user_id         INT,
    status          VARCHAR(50),
    total_amount    NUMERIC(12,2),
    currency        CHAR(3),
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    -- CDC metadata
    _cdc_op         CHAR(1),
    _cdc_lsn        BIGINT,
    _cdc_ts         TIMESTAMPTZ,
    _cdc_tx_id      BIGINT,
    _schema_version INT,
    _dw_loaded_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE TABLE fact_order_items (
    id              INT          NOT NULL,
    order_id        INT,
    sku             VARCHAR(100),
    quantity        INT,
    unit_price      NUMERIC(10,2),
    created_at      TIMESTAMPTZ,
    -- CDC metadata
    _cdc_op         CHAR(1),
    _cdc_lsn        BIGINT,
    _cdc_ts         TIMESTAMPTZ,
    _schema_version INT,
    _dw_loaded_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

-- Audit log: full history of every CDC event applied (for debugging / replays)
CREATE TABLE cdc_audit_log (
    seq             BIGSERIAL    PRIMARY KEY,
    topic           VARCHAR(255) NOT NULL,
    partition       INT,
    kafka_offset    BIGINT,
    table_name      VARCHAR(100),
    record_id       INT,
    op              CHAR(1),
    lsn             BIGINT,
    event_ts        TIMESTAMPTZ,
    applied_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    payload         JSONB
);

CREATE INDEX idx_audit_log_table_id  ON cdc_audit_log (table_name, record_id);
CREATE INDEX idx_audit_log_lsn       ON cdc_audit_log (lsn);
CREATE INDEX idx_audit_log_event_ts  ON cdc_audit_log (event_ts);

-- Dead letter queue for events that failed processing
CREATE TABLE cdc_dead_letter (
    id              BIGSERIAL    PRIMARY KEY,
    topic           VARCHAR(255),
    partition       INT,
    kafka_offset    BIGINT,
    error_message   TEXT,
    raw_payload     JSONB,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Watermark tracking: last safely processed LSN per topic-partition
CREATE TABLE cdc_watermarks (
    topic           VARCHAR(255) NOT NULL,
    partition       INT          NOT NULL,
    last_lsn        BIGINT       NOT NULL DEFAULT 0,
    last_offset     BIGINT       NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic, partition)
);
