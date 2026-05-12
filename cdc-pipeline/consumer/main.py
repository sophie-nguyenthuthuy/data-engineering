"""
CDC Consumer — main entry point.

Flow per Kafka message:
  1. Deserialize with Avro + Schema Registry (gets schema_id automatically)
  2. Migrate record to latest schema via SchemaEvolutionHandler
  3. Buffer in ReorderBuffer (keyed by topic-partition, ordered by LSN)
  4. Periodically flush ready events to WarehouseSink in LSN order
  5. Commit Kafka offsets only after warehouse write succeeds
"""

import logging
import signal
import sys
import time
from typing import List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import CONFIG
from event_processor import ReorderBuffer
from schema_handler import SchemaEvolutionHandler
from warehouse_sink import WarehouseSink

logging.basicConfig(
    level=getattr(logging, CONFIG.log_level),
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
)
log = logging.getLogger("cdc.consumer")

_shutdown = False


def _install_signal_handlers():
    def _handler(sig, frame):
        global _shutdown
        log.info("Received signal %s — shutting down gracefully", sig)
        _shutdown = True

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def _extract_lsn(msg_value: dict) -> int:
    """Pull LSN from the Debezium envelope's source metadata."""
    return int(msg_value.get("__lsn") or msg_value.get("source", {}).get("lsn") or 0)


def _extract_ts_ms(msg_value: dict) -> int:
    return int(msg_value.get("__ts_ms") or msg_value.get("source", {}).get("ts_ms") or 0)


def run():
    _install_signal_handlers()

    schema_registry = SchemaRegistryClient({"url": CONFIG.schema_registry_url})

    # AvroDeserializer automatically reads the schema_id wire-encoded in each message
    deserializer = AvroDeserializer(schema_registry_client=schema_registry)

    schema_handler = SchemaEvolutionHandler(CONFIG.schema_registry_url)
    reorder_buffer = ReorderBuffer(
        lag_tolerance_ms=CONFIG.lag_tolerance_ms,
        max_buffer_size=CONFIG.max_buffer_size,
    )

    sink = WarehouseSink(CONFIG.warehouse_dsn)
    sink.connect()

    consumer = Consumer({
        "bootstrap.servers":       CONFIG.kafka_bootstrap_servers,
        "group.id":                CONFIG.kafka_group_id,
        "auto.offset.reset":       "earliest",
        "enable.auto.commit":      False,      # manual commit after warehouse write
        "max.poll.interval.ms":    300_000,
        "session.timeout.ms":      45_000,
        "heartbeat.interval.ms":   15_000,
        "fetch.max.bytes":         52_428_800,  # 50 MB
        "max.partition.fetch.bytes": 10_485_760,
    })

    consumer.subscribe(CONFIG.topics)
    log.info("Subscribed to topics: %s", CONFIG.topics)

    # Tracks the highest offset seen per (topic, partition) for manual commit
    pending_commits: dict = {}
    last_flush_time = time.monotonic()

    try:
        while not _shutdown:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No new message — check if buffer has aged-out events to flush
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug("Partition EOF: %s [%d]", msg.topic(), msg.partition())
                else:
                    raise KafkaException(msg.error())
            else:
                try:
                    value = deserializer(
                        msg.value(),
                        SerializationContext(msg.topic(), MessageField.VALUE),
                    )
                except Exception as exc:
                    log.error("Deserialization failed on %s[%d]@%d: %s",
                              msg.topic(), msg.partition(), msg.offset(), exc)
                    _route_to_dlq(sink, msg, str(exc))
                    continue

                if value is None:
                    # Tombstone / delete event — Debezium sends null value for hard deletes
                    value = {"__op": "d", "__deleted": "true", "__lsn": 0, "__ts_ms": 0}

                # Schema migration: bring record up to latest schema
                subject = f"{msg.topic()}-value"
                try:
                    schema_id = _get_schema_id_from_wire(msg.value())
                    value = schema_handler.migrate(value, schema_id, subject)
                    value["_schema_version"] = schema_id
                except Exception as exc:
                    log.warning("Schema migration skipped for %s: %s", subject, exc)

                lsn    = _extract_lsn(value)
                ts_ms  = _extract_ts_ms(value)
                ready  = reorder_buffer.add(
                    topic=msg.topic(), partition=msg.partition(),
                    offset=msg.offset(), lsn=lsn, ts_ms=ts_ms, payload=value,
                )

                key = (msg.topic(), msg.partition())
                pending_commits[key] = max(pending_commits.get(key, -1), msg.offset())

                if ready:
                    _flush_and_commit(sink, consumer, ready, pending_commits, reorder_buffer)
                    last_flush_time = time.monotonic()

            # Periodic flush regardless of new messages
            elapsed_ms = (time.monotonic() - last_flush_time) * 1000
            if elapsed_ms >= CONFIG.flush_interval_ms:
                ready = reorder_buffer.drain() if _shutdown else _poll_buffer(reorder_buffer)
                if ready:
                    _flush_and_commit(sink, consumer, ready, pending_commits, reorder_buffer)
                last_flush_time = time.monotonic()

    finally:
        log.info("Draining buffer before shutdown...")
        remaining = reorder_buffer.drain()
        if remaining:
            _flush_and_commit(sink, consumer, remaining, pending_commits, reorder_buffer)
        consumer.close()
        sink.close()
        log.info("Buffer stats: %s", reorder_buffer.stats)
        log.info("Shutdown complete")


def _poll_buffer(buf: ReorderBuffer) -> List[dict]:
    return buf._flush_ready_public()


def _flush_and_commit(sink: WarehouseSink, consumer: Consumer, events: List[dict],
                      pending_commits: dict, buf: ReorderBuffer) -> None:
    applied = sink.apply_batch(events)
    log.info("Applied %d/%d events to warehouse", applied, len(events))

    # Update per-partition watermarks in warehouse
    seen: dict = {}
    for ev in events:
        meta = ev.get("_meta", {})
        key  = (meta.get("topic"), meta.get("partition"))
        if meta.get("lsn", 0) > seen.get(key, (0, 0))[0]:
            seen[key] = (meta["lsn"], meta.get("offset", 0))

    for (topic, part), (lsn, offset) in seen.items():
        if topic and part is not None:
            sink.update_watermark(topic, part, lsn, offset)

    # Commit Kafka offsets (offset+1 = next to consume)
    offsets = [
        TopicPartition(topic, part, offset + 1)
        for (topic, part), offset in pending_commits.items()
    ]
    if offsets:
        consumer.commit(offsets=offsets, asynchronous=False)
        pending_commits.clear()
        log.debug("Committed offsets: %s", offsets)


def _route_to_dlq(sink: WarehouseSink, msg, error: str) -> None:
    try:
        sink.apply_batch([{
            "_meta": {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()},
            "__op": "?", "__deleted": "false",
            "error": error,
        }])
    except Exception:
        pass


def _get_schema_id_from_wire(raw: Optional[bytes]) -> int:
    """Confluent wire format: [0x00][4-byte big-endian schema_id][payload]"""
    if raw and len(raw) >= 5 and raw[0] == 0:
        return int.from_bytes(raw[1:5], "big")
    return 0


if __name__ == "__main__":
    run()
