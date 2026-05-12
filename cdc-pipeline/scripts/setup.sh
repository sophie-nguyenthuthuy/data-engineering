#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."

echo "==> Starting CDC pipeline stack..."
cd "$ROOT"
docker compose up -d --build

echo "==> Waiting for Kafka Connect to be ready..."
until curl -sf http://localhost:8083/connectors > /dev/null; do
    printf '.'
    sleep 3
done
echo ""

echo "==> Registering Debezium connector..."
bash "$SCRIPT_DIR/register_connector.sh"

echo "==> Registering Avro schemas with Schema Registry..."
bash "$SCRIPT_DIR/register_schemas.sh"

echo ""
echo "Pipeline is running."
echo "  Source DB      → localhost:5432 (cdc_source / cdc_secret / transactional_db)"
echo "  Warehouse DB   → localhost:5433 (dw_user / dw_secret / data_warehouse)"
echo "  Kafka          → localhost:9092"
echo "  Schema Registry→ http://localhost:8081"
echo "  Kafka Connect  → http://localhost:8083"
echo ""
echo "Simulate changes:"
echo "  python scripts/simulate_changes.py"
echo ""
echo "Watch warehouse:"
echo "  psql postgresql://dw_user:dw_secret@localhost:5433/data_warehouse -c 'SELECT * FROM dim_users;'"
