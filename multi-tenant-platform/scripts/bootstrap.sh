#!/usr/bin/env bash
# Bootstrap the platform: apply migrations, create platform schema, seed admin tenant.
set -euo pipefail

POSTGRES_URL="${DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/platform}"
PSQL="psql $POSTGRES_URL"

echo "==> Applying migrations..."
$PSQL -f db/migrations/001_initial_schema.sql
$PSQL -f db/migrations/002_partitioning.sql

echo "==> Seeding admin tenant..."
$PSQL <<'SQL'
INSERT INTO platform.tenants (id, name, slug, tier)
VALUES (gen_random_uuid(), 'Platform Admin', 'admin', 'enterprise')
ON CONFLICT (slug) DO NOTHING;
SQL

echo "==> Bootstrap complete."
