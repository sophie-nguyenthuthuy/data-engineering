#!/usr/bin/env bash
# Register the ClickHouse sink connector against the SASL_SSL stack.
# Reads passwords from ./secrets/clients/* directly, injects them into the
# connector config via ${env:FOO} placeholders, and POSTs to Connect.
set -euo pipefail

CONNECT_URL="${CONNECT_URL_EXTERNAL:-http://localhost:8083}"
CONFIG_FILE="${CONFIG_FILE:-infra/kafka-connect/clickhouse-sink.secure.json}"
SECRETS_DIR="${SECRETS_DIR:-secrets}"

require_file() {
  [[ -s "$1" ]] || { echo "missing secret file: $1" >&2; exit 1; }
}

require_file "$SECRETS_DIR/clients/connect_password"
require_file "$SECRETS_DIR/clients/clickhouse_pipeline_password"
require_file "$SECRETS_DIR/clients/truststore_password"

# Expose the values under the names clickhouse-sink.secure.json references.
export CONNECT_KAFKA_PASSWORD="$(cat "$SECRETS_DIR/clients/connect_password")"
export CONNECT_SR_PASSWORD="$CONNECT_KAFKA_PASSWORD"  # same identity, same pw
export CLICKHOUSE_PIPELINE_PASSWORD="$(cat "$SECRETS_DIR/clients/clickhouse_pipeline_password")"
export TRUSTSTORE_PASSWORD="$(cat "$SECRETS_DIR/clients/truststore_password")"

expand_config() {
  python3 -c '
import json, os, re, sys
pattern = re.compile(r"\$\{env:([A-Z0-9_]+)\}")
with open(sys.argv[1]) as f:
    raw = f.read()
def repl(m):
    v = os.environ.get(m.group(1))
    if v is None:
        sys.exit(f"missing env var for connector config: {m.group(1)}")
    # Escape backslashes and quotes for JSON-safety.
    return v.replace("\\", "\\\\").replace("\"", "\\\"")
print(pattern.sub(repl, raw))
' "$CONFIG_FILE"
}

echo "Waiting for Kafka Connect at ${CONNECT_URL}..."
for _ in $(seq 1 60); do
  curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1 && break
  sleep 2
done

body_json="$(expand_config)"
name="$(echo "$body_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["name"])')"
config_only="$(echo "$body_json" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["config"]))')"

echo "Upserting connector '${name}'..."
curl -fsS -X PUT \
  -H "Content-Type: application/json" \
  --data "$config_only" \
  "${CONNECT_URL}/connectors/${name}/config" | python3 -m json.tool
echo
echo "Status:"
curl -fsS "${CONNECT_URL}/connectors/${name}/status" | python3 -m json.tool
