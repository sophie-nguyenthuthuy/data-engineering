#!/usr/bin/env bash
# Provision the ACLs required by the pipeline.
# Runs inside the kafka-init container with admin creds already configured.
# Idempotent — `--add` of an existing ACL is a no-op.
set -euo pipefail

: "${KAFKA_TOPIC:?}" "${KAFKA_DLQ_TOPIC:?}"

KA="/opt/bitnami/kafka/bin/kafka-acls.sh"
CONF="${KAFKA_ACLS_CLIENT_CONF:-/tmp/client.properties}"
BOOT="${KAFKA_BOOTSTRAP:-kafka:9092}"

add() { $KA --bootstrap-server "$BOOT" --command-config "$CONF" --add "$@"; }

echo "==> producer"
add --allow-principal "User:producer" \
    --operation Write --operation Describe \
    --topic "${KAFKA_TOPIC}"
add --allow-principal "User:producer" \
    --operation IdempotentWrite --cluster

echo "==> kafka-connect worker (internal state + source + DLQ + sink groups)"
# Internal _connect_configs/_offsets/_statuses — Create lives on the topic.
add --allow-principal "User:connect" \
    --operation All \
    --resource-pattern-type prefixed \
    --topic "_connect_"
# The worker's own group ID holds the connector status offsets.
add --allow-principal "User:connect" \
    --operation Read --operation Describe \
    --group "pipeline-connect-cluster"

# Source + DLQ.
add --allow-principal "User:connect" \
    --operation Read --operation Describe \
    --topic "${KAFKA_TOPIC}"
add --allow-principal "User:connect" \
    --operation Write --operation Describe --operation Create \
    --topic "${KAFKA_DLQ_TOPIC}"
add --allow-principal "User:connect" \
    --operation IdempotentWrite --cluster

# Sink consumer groups. Each connector uses group ``connect-<connector-name>``.
add --allow-principal "User:connect" \
    --operation Read --operation Describe \
    --resource-pattern-type prefixed \
    --group "connect-"

echo "==> schema-registry"
add --allow-principal "User:schemaregistry" \
    --operation Read --operation Write --operation Describe \
    --topic "_schemas"
add --allow-principal "User:schemaregistry" \
    --operation Read --operation Describe \
    --group "schema-registry"

echo "==> replay (DLQ -> source, Phase 4)"
# Read the DLQ + write replays back onto the source topic. The Dagster
# dlq_replay job uses this principal. Scoped narrowly so a compromised
# replay worker can't talk to anything else.
add --allow-principal "User:replay" \
    --operation Read --operation Describe \
    --topic "${KAFKA_DLQ_TOPIC}"
add --allow-principal "User:replay" \
    --operation Write --operation Describe \
    --topic "${KAFKA_TOPIC}"
add --allow-principal "User:replay" \
    --operation IdempotentWrite --cluster
add --allow-principal "User:replay" \
    --operation Read --operation Describe \
    --group "dlq-replay"

echo
echo "Current ACLs:"
$KA --bootstrap-server "$BOOT" --command-config "$CONF" --list
