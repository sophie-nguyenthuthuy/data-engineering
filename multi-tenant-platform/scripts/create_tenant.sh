#!/usr/bin/env bash
# Usage: ./scripts/create_tenant.sh <name> <slug> <tier>
set -euo pipefail

NAME="${1:?Usage: $0 <name> <slug> <tier>}"
SLUG="${2:?}"
TIER="${3:-free}"
API_URL="${API_URL:-http://localhost:8000}"
ADMIN_TOKEN="${ADMIN_TOKEN:-admin-secret}"

echo "==> Creating tenant: $NAME ($SLUG) — tier: $TIER"

curl -s -X POST "$API_URL/admin/tenants" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: $ADMIN_TOKEN" \
  -d "{\"name\": \"$NAME\", \"slug\": \"$SLUG\", \"tier\": \"$TIER\"}" \
  | python3 -m json.tool

echo ""
echo "==> Done. Tenant bucket created in MinIO."
