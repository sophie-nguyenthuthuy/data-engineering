"""
Kafka producer that publishes synthetic transaction events.
Sends events at ~100/s; after DRIFT_AFTER_EVENTS events the distribution shifts.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time

from confluent_kafka import Producer

from simulator.data_generator import DRIFT_AFTER_EVENTS, generate_event

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_TOPIC = "raw-events"
EVENTS_PER_SECOND = 50


def delivery_report(err, msg) -> None:
    if err:
        logger.error("Delivery failed for %s: %s", msg.key(), err)


def main() -> None:
    bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = Producer({"bootstrap.servers": bs})

    running = True

    def _shutdown(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    seq = 0
    logger.info(
        "Producer started — topic '%s', drift onset after %d events",
        KAFKA_TOPIC,
        DRIFT_AFTER_EVENTS,
    )

    while running:
        event = generate_event(seq)
        producer.produce(
            KAFKA_TOPIC,
            key=event["user_id"].encode(),
            value=json.dumps(event).encode(),
            callback=delivery_report,
        )
        producer.poll(0)
        seq += 1

        if seq % 100 == 0:
            drifted = seq > DRIFT_AFTER_EVENTS
            logger.info(
                "Published %d events (drifted=%s)", seq, drifted
            )

        time.sleep(1.0 / EVENTS_PER_SECOND)

    producer.flush()
    logger.info("Producer stopped after %d events.", seq)


if __name__ == "__main__":
    main()
