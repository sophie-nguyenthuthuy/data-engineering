#!/usr/bin/env bash
# Show Kafka consumer-group lag and watermark state from the warehouse.
set -euo pipefail

CONNECT_URL="http://localhost:8083"
WAREHOUSE_DSN="postgresql://dw_user:dw_secret@localhost:5433/data_warehouse"

echo "==================================================================="
echo " Kafka Connect — connector status"
echo "==================================================================="
curl -s "$CONNECT_URL/connectors/postgres-cdc-connector/status" | jq '
  {
    connector: .connector.state,
    tasks: [ .tasks[] | {id: .id, state: .state} ]
  }'

echo ""
echo "==================================================================="
echo " Consumer group lag  (cdc-consumer-group)"
echo "==================================================================="
docker compose exec kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group cdc-consumer-group 2>/dev/null || \
  echo "(consumer not running or not yet committed any offsets)"

echo ""
echo "==================================================================="
echo " Warehouse watermarks (last processed LSN per partition)"
echo "==================================================================="
psql "$WAREHOUSE_DSN" -x -c "
  SELECT topic, partition, last_lsn, last_offset,
         NOW() - updated_at AS age
  FROM cdc_watermarks
  ORDER BY topic, partition;"

echo ""
echo "==================================================================="
echo " Warehouse row counts"
echo "==================================================================="
psql "$WAREHOUSE_DSN" -c "
  SELECT 'dim_users'        AS table_name, COUNT(*) AS rows FROM dim_users
  UNION ALL
  SELECT 'fact_orders',                    COUNT(*)          FROM fact_orders
  UNION ALL
  SELECT 'fact_order_items',               COUNT(*)          FROM fact_order_items
  UNION ALL
  SELECT 'cdc_audit_log',                  COUNT(*)          FROM cdc_audit_log
  UNION ALL
  SELECT 'cdc_dead_letter',                COUNT(*)          FROM cdc_dead_letter
  ORDER BY table_name;"

echo ""
echo "==================================================================="
echo " Recent audit events (last 10)"
echo "==================================================================="
psql "$WAREHOUSE_DSN" -c "
  SELECT applied_at, table_name, record_id, op, lsn
  FROM cdc_audit_log
  ORDER BY seq DESC
  LIMIT 10;"

echo ""
echo "==================================================================="
echo " Dead-letter queue"
echo "==================================================================="
psql "$WAREHOUSE_DSN" -c "
  SELECT created_at, topic, partition, kafka_offset, error_message
  FROM cdc_dead_letter
  ORDER BY id DESC
  LIMIT 5;"
