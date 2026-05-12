from __future__ import annotations

import json
import logging
import signal
import sys
import time
from pathlib import Path
from types import FrameType

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from .config import Config
from .events import make_event
from .obs import setup as setup_obs
from .schema_check import ensure_compatible, subject_for


def _configure_logging() -> structlog.stdlib.BoundLogger:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
    )
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    return logger


def _load_schema(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def run(cfg: Config) -> None:
    log = _configure_logging()
    instruments = setup_obs()
    log.info("producer.start", brokers=cfg.brokers, topic=cfg.topic, rate=cfg.events_per_sec)

    sr_config: dict[str, str] = {"url": cfg.schema_registry_url}
    if cfg.sasl.sr_username and cfg.sasl.sr_password:
        sr_config["basic.auth.user.info"] = f"{cfg.sasl.sr_username}:{cfg.sasl.sr_password}"
    sr = SchemaRegistryClient(sr_config)

    schema_str = _load_schema(cfg.schema_path)
    subject = subject_for(cfg.topic)
    log.info("producer.schema_check", subject=subject)
    ensure_compatible(sr, subject, schema_str)  # fails fast on breaking change

    value_serializer = AvroSerializer(sr, schema_str)
    key_serializer = StringSerializer("utf_8")

    producer_conf: dict[str, object] = {
        "bootstrap.servers": cfg.brokers,
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "zstd",
        "linger.ms": 20,
        "batch.size": 64 * 1024,
        "retries": 10,
        "client.id": "pipeline-producer",
    }
    if cfg.sasl.enabled:
        producer_conf.update(
            {
                "security.protocol": cfg.sasl.security_protocol,
                "sasl.mechanism": cfg.sasl.mechanism or "SCRAM-SHA-512",
                "sasl.username": cfg.sasl.username or "",
                "sasl.password": cfg.sasl.password or "",
            }
        )
        if cfg.sasl.ca_location:
            producer_conf["ssl.ca.location"] = cfg.sasl.ca_location
    producer = Producer(producer_conf)

    def on_delivery(err: Exception | None, msg: object) -> None:
        if err is not None:
            instruments["delivery_failed"].add(1)
            log.error("producer.delivery_failed", error=str(err))

    stop = False

    def _handle_signal(signum: int, _frame: FrameType | None) -> None:
        nonlocal stop
        log.info("producer.signal", signal=signum)
        stop = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    interval = 1.0 / max(cfg.events_per_sec, 0.01)
    ctx = SerializationContext(cfg.topic, MessageField.VALUE)
    sent = 0
    next_tick = time.monotonic()

    while not stop:
        evt = make_event(error_rate=cfg.error_rate)
        try:
            payload = value_serializer(evt, ctx)
            producer.produce(
                topic=cfg.topic,
                key=key_serializer(evt["user_id"]),
                value=payload,
                on_delivery=on_delivery,
            )
            sent += 1
            instruments["events_sent"].add(1, {"event_type": evt["event_type"]})
        except BufferError:
            producer.poll(0.5)
            continue
        except Exception as exc:
            log.error(
                "producer.serialize_failed",
                error=str(exc),
                payload=json.dumps(evt, default=str),
            )

        producer.poll(0)

        if sent % 500 == 0:
            log.info("producer.progress", sent=sent)

        next_tick += interval
        sleep = next_tick - time.monotonic()
        if sleep > 0:
            time.sleep(sleep)
        else:
            next_tick = time.monotonic()

    log.info("producer.flushing", pending=len(producer))
    producer.flush(15)
    log.info("producer.stopped", sent=sent)


def main() -> None:
    run(Config.from_env())


if __name__ == "__main__":
    main()
