#!/usr/bin/env bash
# Peek up to N messages from the DLQ, with error-context headers.
#
#   scripts/inspect-dlq.sh           # 10 messages, secure stack
#   scripts/inspect-dlq.sh 50        # 50 messages, secure stack
#   STACK=dev scripts/inspect-dlq.sh # against the dev stack
set -euo pipefail

MAX="${1:-10}"
STACK="${STACK:-secure}"
DLQ="${KAFKA_DLQ_TOPIC:-user-interactions-dlq}"

# shellcheck disable=SC2016
BASE_ARGS=(
  --bootstrap-server kafka:9092
  --topic "$DLQ"
  --from-beginning
  --max-messages "$MAX"
  --property print.headers=true
  --property print.offset=true
  --property print.partition=true
  --property print.timestamp=true
  --timeout-ms 10000
)

if [[ "$STACK" == "secure" ]]; then
  [[ -s secrets/clients/admin_password ]] || { echo "run make bootstrap first" >&2; exit 1; }
  ADMIN_PW="$(cat secrets/clients/admin_password)"
  PROPS="$(mktemp)"
  trap 'rm -f "$PROPS"' EXIT
  cat >"$PROPS" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${ADMIN_PW}";
ssl.truststore.type=PEM
ssl.truststore.location=/opt/bitnami/kafka/config/certs/kafka.truststore.pem
EOF
  docker cp "$PROPS" kafka:/tmp/dlq-client.properties >/dev/null
  BASE_ARGS+=(--consumer.config /tmp/dlq-client.properties)
fi

echo "Peeking up to ${MAX} messages from ${DLQ} (${STACK} stack)..."
docker exec -i kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh "${BASE_ARGS[@]}" 2>&1 || true
