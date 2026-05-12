#!/usr/bin/env bash
set -euo pipefail

SR_URL="http://localhost:8081"
SCHEMAS_DIR="$(dirname "$0")/../schemas"

register() {
    local subject="$1"
    local file="$2"
    local schema
    schema=$(jq -c . "$file")
    echo "==> Registering $subject from $file"
    curl -s -X POST "$SR_URL/subjects/$subject/versions" \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": $(echo "$schema" | jq -Rs .)}" | jq .
}

# Set compatibility to BACKWARD before registering
for subject in "cdc.public.users-value" "cdc.public.orders-value" "cdc.public.order_items-value"; do
    curl -s -X PUT "$SR_URL/config/$subject" \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d '{"compatibility": "BACKWARD"}' | jq .
done

register "cdc.public.users-value"             "$SCHEMAS_DIR/users_v1.avsc"
register "cdc.public.orders-value"            "$SCHEMAS_DIR/orders_v1.avsc"
register "cdc.public.order_items-value"       "$SCHEMAS_DIR/order_items_v1.avsc"

echo ""
echo "==> Registered schemas:"
curl -s "$SR_URL/subjects" | jq .

echo ""
echo "To evolve the users schema to v2, run:"
echo "  bash scripts/evolve_schema.sh"
