#!/usr/bin/env bash
# Register the ClickHouse sink connector with Kafka Connect.
# Polls until Connect is healthy, then PUTs the config (idempotent).
set -euo pipefail

CONNECT_URL="${CONNECT_URL_EXTERNAL:-http://localhost:8083}"
CONFIG_FILE="${CONFIG_FILE:-infra/kafka-connect/clickhouse-sink.json}"

# Expand ${env:FOO} tokens in the config against the current shell env,
# so we don't duplicate values between compose and the connector JSON.
expand_config() {
  python3 -c '
import json, os, re, sys
pattern = re.compile(r"\$\{env:([A-Z0-9_]+)\}")
with open(sys.argv[1]) as f:
    raw = f.read()
def repl(m):
    v = os.environ.get(m.group(1))
    if v is None:
        sys.exit(f"missing env var: {m.group(1)}")
    return v
print(pattern.sub(repl, raw))
' "$CONFIG_FILE"
}

echo "Waiting for Kafka Connect at ${CONNECT_URL}..."
for _ in $(seq 1 60); do
  if curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

config_json="$(expand_config)"
name="$(echo "$config_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["name"])')"
config_only="$(echo "$config_json" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["config"]))')"

echo "Upserting connector '${name}'..."
curl -fsS -X PUT \
  -H "Content-Type: application/json" \
  --data "$config_only" \
  "${CONNECT_URL}/connectors/${name}/config" | python3 -m json.tool
echo
echo "Done. Status:"
curl -fsS "${CONNECT_URL}/connectors/${name}/status" | python3 -m json.tool
