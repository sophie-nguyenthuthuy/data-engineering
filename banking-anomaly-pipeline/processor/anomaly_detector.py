"""
Real-time anomaly detector — Kafka consumer → fraud rules → Kafka producer.

Architecture note: this service mirrors what a PySpark Structured Streaming job
would do, implemented as a pure-Python micro-batch processor so it runs without a
Spark cluster. A drop-in PySpark version (spark_detector.py) is included alongside
this file for cluster deployments.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer

from fraud_rules import FraudDetector, FraudSignal, aggregate_risk

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "transactions")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "fraud-alerts")
STATS_TOPIC = os.getenv("STATS_TOPIC", "tx-stats")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

BATCH_INTERVAL_SEC = 1.0  # micro-batch window


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "anomaly-detector-v1",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "session.timeout.ms": 30000,
    })


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "queue.buffering.max.ms": 100,
    })


def delivery_report(err, msg):
    if err:
        print(f"[detector] delivery error: {err}", file=sys.stderr)


def process_batch(
    messages: list[dict],
    detector: FraudDetector,
    producer: Producer,
    stats: dict,
) -> None:
    for tx in messages:
        signals = detector.analyze(tx)
        stats["total"] += 1
        stats["by_category"][tx.get("merchant_category", "unknown")] = (
            stats["by_category"].get(tx.get("merchant_category", "unknown"), 0) + 1
        )

        if not signals:
            continue

        risk_score, severity = aggregate_risk(signals)
        stats["alerts"] += 1

        alert = {
            "alert_id": f"ALT-{tx['transaction_id'][:8].upper()}",
            "transaction_id": tx["transaction_id"],
            "account_id": tx["account_id"],
            "amount": tx["amount"],
            "merchant": tx.get("merchant"),
            "city": tx.get("city"),
            "risk_score": risk_score,
            "severity": severity,
            "signals": [
                {
                    "rule": s.rule,
                    "severity": s.severity,
                    "score": s.score,
                    "detail": s.detail,
                    "metadata": s.metadata,
                }
                for s in signals
            ],
            "original_timestamp": tx.get("timestamp"),
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }

        payload = json.dumps(alert).encode()
        producer.produce(
            ALERTS_TOPIC,
            key=tx["account_id"].encode(),
            value=payload,
            callback=delivery_report,
        )

        severity_label = severity.ljust(8)
        rules = ", ".join(s.rule for s in signals)
        print(f"[ALERT] {severity_label} score={risk_score:3d}  {tx['account_id']}  "
              f"${tx['amount']:>10,.2f}  rules=[{rules}]")

    # Publish micro-batch stats
    if stats["total"] % 50 == 0 and stats["total"] > 0:
        stat_event = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "total_processed": stats["total"],
            "total_alerts": stats["alerts"],
            "alert_rate_pct": round(stats["alerts"] / stats["total"] * 100, 2),
        }
        producer.produce(STATS_TOPIC, value=json.dumps(stat_event).encode())

    producer.poll(0)


def main():
    consumer = build_consumer()
    producer = build_producer()
    detector = FraudDetector(redis_host=REDIS_HOST, redis_port=REDIS_PORT)

    consumer.subscribe([INPUT_TOPIC])
    print(f"[detector] listening on {INPUT_TOPIC} (bootstrap={KAFKA_BOOTSTRAP})")

    stats = {"total": 0, "alerts": 0, "by_category": {}}
    running = True

    def _shutdown(sig, frame):
        nonlocal running
        print("[detector] shutting down…")
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    batch: list[dict] = []
    deadline = time.monotonic() + BATCH_INTERVAL_SEC

    while running:
        msg = consumer.poll(timeout=0.05)
        if msg is None:
            pass
        elif msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[detector] consumer error: {msg.error()}", file=sys.stderr)
        else:
            try:
                tx = json.loads(msg.value().decode())
                batch.append(tx)
            except Exception as e:
                print(f"[detector] parse error: {e}", file=sys.stderr)

        if time.monotonic() >= deadline:
            if batch:
                process_batch(batch, detector, producer, stats)
                batch = []
            deadline = time.monotonic() + BATCH_INTERVAL_SEC

    consumer.close()
    producer.flush()


if __name__ == "__main__":
    main()
