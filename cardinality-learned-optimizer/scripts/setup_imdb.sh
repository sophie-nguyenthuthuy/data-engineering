#!/usr/bin/env bash
# Download and load the IMDB dataset into PostgreSQL.
# Prerequisites: psql, wget/curl, PostgreSQL running on $PG_HOST:$PG_PORT
#
# Full IMDB schema: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/2QYZBT
# (Leis et al., 2015 — JOB benchmark dataset)
#
# Quick mirror: https://homepages.cwi.nl/~boncz/job/imdb.tgz  (~1.1 GB)

set -euo pipefail

PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
PG_DBNAME="${PG_DBNAME:-imdb}"
DATA_DIR="${DATA_DIR:-/tmp/imdb_data}"
IMDB_URL="https://homepages.cwi.nl/~boncz/job/imdb.tgz"

echo "==> Creating database $PG_DBNAME"
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -c "CREATE DATABASE $PG_DBNAME;" 2>/dev/null || echo "(already exists)"

echo "==> Downloading IMDB data → $DATA_DIR"
mkdir -p "$DATA_DIR"
if [ ! -f "$DATA_DIR/imdb.tgz" ]; then
    wget -O "$DATA_DIR/imdb.tgz" "$IMDB_URL" \
        || curl -L -o "$DATA_DIR/imdb.tgz" "$IMDB_URL"
fi

echo "==> Extracting"
cd "$DATA_DIR"
tar -xzf imdb.tgz 2>/dev/null || true

echo "==> Loading schema"
SCHEMA_URL="https://raw.githubusercontent.com/gregrahn/join-order-benchmark/master/schema.sql"
if [ ! -f "$DATA_DIR/schema.sql" ]; then
    wget -O "$DATA_DIR/schema.sql" "$SCHEMA_URL" \
        || curl -L -o "$DATA_DIR/schema.sql" "$SCHEMA_URL"
fi
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DBNAME" -f "$DATA_DIR/schema.sql"

echo "==> Loading CSV data (this takes ~10 minutes)"
for csv_file in "$DATA_DIR"/*.csv; do
    table=$(basename "$csv_file" .csv)
    echo "  COPY $table"
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DBNAME" \
        -c "\\COPY $table FROM '$csv_file' CSV ESCAPE '\\'"  2>/dev/null || \
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DBNAME" \
        -c "\\COPY $table FROM '$csv_file' CSV"
done

echo "==> Creating indexes (ANALYZE)"
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DBNAME" -c "ANALYZE;"

echo "==> Installing pg_hint_plan (if available)"
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DBNAME" \
    -c "CREATE EXTENSION IF NOT EXISTS pg_hint_plan;" 2>/dev/null \
    || echo "(pg_hint_plan not available — install from https://github.com/ossc-db/pg_hint_plan)"

echo "==> IMDB loaded successfully into $PG_DBNAME"
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DBNAME" -c \
    "SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) FROM pg_tables WHERE schemaname='public' ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"
