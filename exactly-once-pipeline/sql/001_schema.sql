-- ============================================================
-- Exactly-Once Cross-System Transaction Pipeline — Schema
-- ============================================================

-- ── Ledger (source of truth for payments) ───────────────────
CREATE TABLE IF NOT EXISTS ledger (
    id              BIGSERIAL PRIMARY KEY,
    payment_id      UUID        NOT NULL UNIQUE,
    idempotency_key UUID        NOT NULL UNIQUE,
    sender_account  TEXT        NOT NULL,
    receiver_account TEXT       NOT NULL,
    amount          NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    currency        CHAR(3)     NOT NULL DEFAULT 'USD',
    description     TEXT,
    status          TEXT        NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ledger_idempotency ON ledger(idempotency_key);
CREATE INDEX idx_ledger_status      ON ledger(status);

-- ── Outbox (written atomically with ledger, polled by relay) ─
CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key UUID        NOT NULL,
    aggregate_type  TEXT        NOT NULL DEFAULT 'payment',
    aggregate_id    UUID        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,             -- NULL → not yet published
    retry_count     INT         NOT NULL DEFAULT 0,
    last_error      TEXT
);

CREATE INDEX idx_outbox_unpublished ON outbox(created_at) WHERE published_at IS NULL;
CREATE INDEX idx_outbox_idempotency  ON outbox(idempotency_key);

-- ── Distributed Transaction Coordinator ─────────────────────
CREATE TABLE IF NOT EXISTS transaction_states (
    transaction_id  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key UUID        NOT NULL UNIQUE,
    payment_id      UUID        NOT NULL,
    current_step    TEXT        NOT NULL DEFAULT 'CREATED',
    kafka_published BOOLEAN     NOT NULL DEFAULT FALSE,
    warehouse_ack   BOOLEAN     NOT NULL DEFAULT FALSE,
    notification_ack BOOLEAN    NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    failed_at       TIMESTAMPTZ,
    error_message   TEXT,
    retry_count     INT         NOT NULL DEFAULT 0,
    metadata        JSONB       NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_txstate_step ON transaction_states(current_step);
CREATE INDEX idx_txstate_payment ON transaction_states(payment_id);

-- ── Idempotency registry (shared across all consumers) ──────
CREATE TABLE IF NOT EXISTS idempotency_log (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key UUID        NOT NULL,
    consumer        TEXT        NOT NULL,   -- 'warehouse' | 'notification'
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(idempotency_key, consumer)
);

-- ── Warehouse (analytics / append-only ledger copy) ─────────
CREATE TABLE IF NOT EXISTS warehouse_payments (
    id              BIGSERIAL PRIMARY KEY,
    payment_id      UUID        NOT NULL UNIQUE,
    idempotency_key UUID        NOT NULL UNIQUE,
    sender_account  TEXT        NOT NULL,
    receiver_account TEXT       NOT NULL,
    amount          NUMERIC(18,2) NOT NULL,
    currency        CHAR(3)     NOT NULL,
    description     TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_metadata  JSONB       NOT NULL DEFAULT '{}'
);

-- ── Notification queue (durable log of sent notifications) ───
CREATE TABLE IF NOT EXISTS notification_log (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key UUID        NOT NULL UNIQUE,
    payment_id      UUID        NOT NULL,
    channel         TEXT        NOT NULL DEFAULT 'email',
    recipient       TEXT        NOT NULL,
    subject         TEXT        NOT NULL,
    body            TEXT        NOT NULL,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Helper: auto-update updated_at ──────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_updated_at
    BEFORE UPDATE ON ledger
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER txstate_updated_at
    BEFORE UPDATE ON transaction_states
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
