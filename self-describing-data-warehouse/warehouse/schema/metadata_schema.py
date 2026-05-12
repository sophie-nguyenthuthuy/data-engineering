"""
SQL schema for the metadata layer — the "data about data" tables.
"""

METADATA_SCHEMA = """
-- Core table registry
CREATE TABLE IF NOT EXISTS meta_tables (
    table_name      TEXT PRIMARY KEY,
    description     TEXT NOT NULL,
    owner           TEXT NOT NULL,
    domain          TEXT NOT NULL,         -- e.g. 'finance', 'product', 'marketing'
    source_system   TEXT,                  -- e.g. 'Salesforce', 'Stripe', 'internal'
    update_frequency TEXT,                 -- e.g. 'daily', 'hourly', 'realtime'
    tags            TEXT,                  -- JSON array of strings
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    is_deprecated   INTEGER DEFAULT 0,
    deprecation_note TEXT
);

-- Column-level metadata
CREATE TABLE IF NOT EXISTS meta_columns (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name      TEXT NOT NULL,
    column_name     TEXT NOT NULL,
    data_type       TEXT NOT NULL,
    description     TEXT,
    is_pii          INTEGER DEFAULT 0,
    is_nullable     INTEGER DEFAULT 1,
    sample_values   TEXT,                  -- JSON array of example values
    FOREIGN KEY (table_name) REFERENCES meta_tables(table_name),
    UNIQUE(table_name, column_name)
);

-- Lineage: directed edges between tables (upstream -> downstream)
CREATE TABLE IF NOT EXISTS meta_lineage (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    upstream_table  TEXT NOT NULL,
    downstream_table TEXT NOT NULL,
    transformation  TEXT,                  -- description of how data flows
    created_at      TEXT NOT NULL,
    FOREIGN KEY (upstream_table)   REFERENCES meta_tables(table_name),
    FOREIGN KEY (downstream_table) REFERENCES meta_tables(table_name),
    UNIQUE(upstream_table, downstream_table)
);

-- Quality check results (one row per check run)
CREATE TABLE IF NOT EXISTS meta_quality_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name      TEXT NOT NULL,
    run_at          TEXT NOT NULL,
    row_count       INTEGER,
    null_rate       REAL,                  -- avg null % across all columns
    duplicate_rate  REAL,                  -- % duplicate rows
    constraint_violations INTEGER,
    quality_score   REAL,                  -- 0-100
    notes           TEXT,
    FOREIGN KEY (table_name) REFERENCES meta_tables(table_name)
);

-- Freshness snapshots
CREATE TABLE IF NOT EXISTS meta_freshness (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name      TEXT NOT NULL,
    checked_at      TEXT NOT NULL,
    last_updated_at TEXT,
    expected_interval_hours REAL,
    hours_since_update REAL,
    freshness_score REAL,                  -- 0-100
    FOREIGN KEY (table_name) REFERENCES meta_tables(table_name)
);

-- Usage / query tracking
CREATE TABLE IF NOT EXISTS meta_usage (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name      TEXT NOT NULL,
    queried_at      TEXT NOT NULL,
    queried_by      TEXT,
    query_preview   TEXT,                  -- first 200 chars of the query
    execution_ms    INTEGER,
    FOREIGN KEY (table_name) REFERENCES meta_tables(table_name)
);

-- Incident log (what broke it last)
CREATE TABLE IF NOT EXISTS meta_incidents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name      TEXT NOT NULL,
    occurred_at     TEXT NOT NULL,
    resolved_at     TEXT,
    severity        TEXT,                  -- 'low', 'medium', 'high', 'critical'
    description     TEXT NOT NULL,
    root_cause      TEXT,
    resolved_by     TEXT,
    FOREIGN KEY (table_name) REFERENCES meta_tables(table_name)
);
"""
