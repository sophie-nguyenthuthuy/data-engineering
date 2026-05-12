#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="http://localhost:8083"
CONNECTOR_FILE="$(dirname "$0")/../config/debezium/postgres-connector.json"

echo "==> Checking for existing connector..."
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors/postgres-cdc-connector")

if [ "$STATUS" = "200" ]; then
    echo "    Connector exists — updating..."
    curl -s -X PUT "$CONNECT_URL/connectors/postgres-cdc-connector/config" \
        -H "Content-Type: application/json" \
        -d "$(jq '.config' "$CONNECTOR_FILE")" | jq .
else
    echo "    Creating connector..."
    curl -s -X POST "$CONNECT_URL/connectors" \
        -H "Content-Type: application/json" \
        -d @"$CONNECTOR_FILE" | jq .
fi

echo ""
echo "==> Connector status:"
sleep 2
curl -s "$CONNECT_URL/connectors/postgres-cdc-connector/status" | jq .
