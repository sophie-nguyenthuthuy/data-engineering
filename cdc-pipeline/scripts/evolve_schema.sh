#!/usr/bin/env bash
# Demonstrates schema evolution: register users_v2 and ALTER the source table.
set -euo pipefail

SR_URL="http://localhost:8081"
SCHEMAS_DIR="$(dirname "$0")/../schemas"
SOURCE_DB="postgresql://cdc_source:cdc_secret@localhost:5432/transactional_db"

echo "==> Step 1: Register users schema v2 (adds tier, phone; renames status→account_status)"
SCHEMA=$(jq -c . "$SCHEMAS_DIR/users_v2.avsc")
curl -s -X POST "$SR_URL/subjects/cdc.public.users-value/versions" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schema\": $(echo "$SCHEMA" | jq -Rs .)}" | jq .

echo ""
echo "==> Step 2: ALTER source table to match v2 schema"
psql "$SOURCE_DB" <<'SQL'
BEGIN;
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS tier  VARCHAR(50),
    ADD COLUMN IF NOT EXISTS phone VARCHAR(50);

-- Rename status → account_status (Debezium will emit the new column name)
ALTER TABLE users RENAME COLUMN status TO account_status;

-- Populate tier for existing users
UPDATE users SET tier = 'free' WHERE tier IS NULL;

COMMIT;
SQL

echo ""
echo "==> Step 3: Verify new columns appear in CDC stream"
echo "    Insert a v2 user to trigger a new event:"
psql "$SOURCE_DB" <<'SQL'
INSERT INTO users (email, username, account_status, tier, phone)
VALUES ('frank@example.com', 'frank', 'active', 'pro', '+1-555-0100');
SQL

echo ""
echo "Schema evolution complete. The consumer's SchemaEvolutionHandler will:"
echo "  - Detect schema_id mismatch between stored and latest"
echo "  - Apply field aliases (status → account_status)"
echo "  - Fill tier/phone with null for old records"
