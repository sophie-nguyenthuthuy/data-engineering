"""
Kafka → GCS consumer.

Consumes the gh_events topic, buffers events per 5-minute window, and flushes
each window as a single gzipped JSONL object to
  gs://$GCS_BUCKET/stream/dt=YYYY-MM-DD/hour=HH/events-YYYYMMDDHHMM.jsonl.gz

BigQuery can read this directly as an external table (see `make bq-external`).
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import signal
import sys
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer
from google.cloud import storage

log = logging.getLogger("consumer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")

WINDOW_MINUTES = 5


def _window_key(ts: datetime) -> str:
    m = (ts.minute // WINDOW_MINUTES) * WINDOW_MINUTES
    return ts.replace(minute=m, second=0, microsecond=0).strftime("%Y%m%d%H%M")


def _object_path(window_key: str) -> str:
    dt = datetime.strptime(window_key, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    return f"stream/dt={dt:%Y-%m-%d}/hour={dt:%H}/events-{window_key}.jsonl.gz"


def flush(client: storage.Client, bucket: str, window_key: str, rows: list[bytes]) -> None:
    if not rows:
        return
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for r in rows:
            gz.write(r)
            gz.write(b"\n")
    buf.seek(0)
    path = _object_path(window_key)
    client.bucket(bucket).blob(path).upload_from_file(
        buf, content_type="application/gzip", size=len(buf.getvalue())
    )
    log.info("flushed %d rows → gs://%s/%s", len(rows), bucket, path)


def main() -> None:
    bucket = os.environ["GCS_BUCKET"]
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
    topic = os.environ.get("KAFKA_TOPIC", "gh_events")

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "gh-consumer-gcs",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    gcs = storage.Client()
    buffers: dict[str, list[bytes]] = defaultdict(list)
    last_window: str | None = None

    stop = False
    def _shutdown(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("consuming %s from %s → gs://%s/stream/", topic, bootstrap, bucket)

    while not stop:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            now_window = _window_key(datetime.now(timezone.utc))
            if last_window and now_window != last_window and buffers.get(last_window):
                flush(gcs, bucket, last_window, buffers.pop(last_window))
                consumer.commit(asynchronous=False)
            continue
        if msg.error():
            log.error("kafka error: %s", msg.error())
            continue

        try:
            evt = json.loads(msg.value())
            ts = datetime.fromisoformat(evt["created_at"].replace("Z", "+00:00"))
        except Exception:
            ts = datetime.now(timezone.utc)

        wk = _window_key(ts)
        buffers[wk].append(msg.value())
        last_window = wk

        # Belt-and-suspenders: flush if the active window gets huge.
        if len(buffers[wk]) >= 5000:
            flush(gcs, bucket, wk, buffers.pop(wk))
            consumer.commit(asynchronous=False)

    # graceful shutdown — flush every pending window
    for wk, rows in buffers.items():
        flush(gcs, bucket, wk, rows)
    consumer.commit(asynchronous=False)
    consumer.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
