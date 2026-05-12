"""
GitHub Events API → Kafka producer.

Polls https://api.github.com/events (the last ~5 minutes of public events,
300 events paged 30x10) and publishes each event to Kafka. Respects ETag to
avoid wasting rate-limit quota, and honors X-Poll-Interval.

Authenticated token: 5000 req/hr. Unauthenticated: 60/hr.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Iterator

import requests
from confluent_kafka import Producer

log = logging.getLogger("producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")

GITHUB_EVENTS = "https://api.github.com/events"
DEFAULT_POLL = 60  # seconds; server may override via X-Poll-Interval


def _headers(token: str | None, etag: str | None) -> dict:
    h = {"Accept": "application/vnd.github+json", "User-Agent": "gh-pipeline/1.0"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    if etag:
        h["If-None-Match"] = etag
    return h


def poll_events(session: requests.Session, token: str | None, etag: str | None) -> tuple[list[dict], str | None, int]:
    """Fetch one page of events. Returns (events, new_etag, next_poll_seconds)."""
    r = session.get(GITHUB_EVENTS, headers=_headers(token, etag), timeout=30)
    poll = int(r.headers.get("X-Poll-Interval", DEFAULT_POLL))
    if r.status_code == 304:
        return [], etag, poll
    r.raise_for_status()
    return r.json(), r.headers.get("ETag"), poll


def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
    topic = os.environ.get("KAFKA_TOPIC", "gh_events")
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        log.warning("no GITHUB_TOKEN set — you'll hit the 60/hr rate limit quickly")

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 50,
        "compression.type": "zstd",
        "enable.idempotence": True,
    })

    seen: set[str] = set()  # de-dupe across overlapping polls
    etag: str | None = None

    stop = False
    def _shutdown(*_):
        nonlocal stop
        stop = True
        log.info("shutting down…")
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    session = requests.Session()
    log.info("producing to %s / topic %s", bootstrap, topic)

    while not stop:
        try:
            events, etag, poll = poll_events(session, token, etag)
        except requests.HTTPError as e:
            log.error("github error: %s", e)
            time.sleep(30)
            continue

        new = 0
        for evt in events:
            eid = evt.get("id")
            if not eid or eid in seen:
                continue
            seen.add(eid)
            producer.produce(
                topic,
                key=eid.encode(),
                value=json.dumps(evt, separators=(",", ":")).encode(),
            )
            new += 1

        # Keep the seen set from growing unbounded.
        if len(seen) > 20_000:
            seen = set(list(seen)[-10_000:])

        producer.poll(0)
        log.info("polled: %d new events, next in %ds", new, poll)

        # Sleep in small slices so SIGTERM is responsive.
        for _ in range(poll):
            if stop: break
            time.sleep(1)

    producer.flush(10)
    log.info("flushed, exiting")
    sys.exit(0)


if __name__ == "__main__":
    main()
