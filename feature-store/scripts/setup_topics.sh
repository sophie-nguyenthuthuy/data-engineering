#!/usr/bin/env bash
# Create Kafka topics with settings tuned for low-latency feature events.
set -euo pipefail

BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

create_topic() {
  local name="$1"
  local partitions="${2:-12}"
  local retention_ms="${3:-86400000}"   # 24h

  kafka-topics.sh \
    --bootstrap-server "$BROKER" \
    --create \
    --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms="$retention_ms" \
    --config compression.type=snappy \
    --config min.insync.replicas=1

  echo "✓ topic: $name (partitions=$partitions)"
}

echo "Creating feature store Kafka topics on $BROKER..."
create_topic "feature-events"   12 86400000
create_topic "feature-updates"  6  86400000
create_topic "feature-dlq"      3  604800000  # 7d for dead-letter

echo "Done."
