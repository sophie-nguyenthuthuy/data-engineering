"""Backend tests."""

from __future__ import annotations

import json

import pytest

from ifw.backends.inmemory import InMemoryBackend
from ifw.backends.polling import PollingBackend
from ifw.backends.s3_sqs import S3SqsBackend, parse_s3_event
from ifw.events import EventKind, FileEvent

# ---------------------------------------------------------------- InMemory


def test_inmemory_push_and_drain():
    b = InMemoryBackend()
    e = FileEvent(bucket="b", key="k", size=1, last_modified_ms=0, etag="e")
    b.push(e)
    out = list(b.poll())
    assert out == [e]
    # Second poll is empty.
    assert list(b.poll()) == []


# ----------------------------------------------------------------- Polling


def test_polling_rejects_empty_bucket():
    with pytest.raises(ValueError):
        PollingBackend(bucket="", lister=lambda _b: [])


def test_polling_emits_created_then_modified():
    state = {
        "k1": ("k1", 10, 1_000, "etag-v1"),
    }

    def lister(_bucket):
        return list(state.values())

    b = PollingBackend(bucket="b", lister=lister)
    first = list(b.poll())
    assert len(first) == 1
    assert first[0].kind == EventKind.CREATED

    # Same etag → no new event.
    assert list(b.poll()) == []

    # ETag bump → MODIFIED.
    state["k1"] = ("k1", 10, 2_000, "etag-v2")
    second = list(b.poll())
    assert len(second) == 1
    assert second[0].kind == EventKind.MODIFIED
    assert second[0].etag == "etag-v2"


def test_polling_detects_new_keys():
    state: dict[str, tuple[str, int, int, str]] = {}

    def lister(_bucket):
        return list(state.values())

    b = PollingBackend(bucket="b", lister=lister)
    state["k1"] = ("k1", 10, 1, "e1")
    state["k2"] = ("k2", 20, 2, "e2")
    out = list(b.poll())
    assert {e.key for e in out} == {"k1", "k2"}


# ----------------------------------------------------------------- S3 SQS


def _sqs_body(
    bucket="b",
    key="k",
    etag='"d41d8cd98f00b204e9800998ecf8427e"',
    size=12,
    name="ObjectCreated:Put",
    ts="2024-01-01T12:00:00.000Z",
):
    return json.dumps(
        {
            "Records": [
                {
                    "eventName": name,
                    "eventTime": ts,
                    "s3": {
                        "bucket": {"name": bucket},
                        "object": {"key": key, "size": size, "eTag": etag},
                    },
                }
            ]
        }
    ).encode()


def test_parse_s3_event_basic():
    events = parse_s3_event(_sqs_body())
    assert len(events) == 1
    assert events[0].bucket == "b"
    assert events[0].key == "k"
    assert events[0].kind == EventKind.CREATED


def test_parse_s3_event_deleted_kind():
    events = parse_s3_event(_sqs_body(name="ObjectRemoved:Delete"))
    assert events[0].kind == EventKind.DELETED


def test_parse_s3_event_invalid_json_raises():
    with pytest.raises(ValueError):
        parse_s3_event(b"not json")


def test_parse_s3_event_missing_field_raises():
    bad = json.dumps({"Records": [{"s3": {"bucket": {"name": "b"}, "object": {}}}]}).encode()
    with pytest.raises(ValueError):
        parse_s3_event(bad)


def test_parse_s3_event_handles_iso_with_z_suffix():
    events = parse_s3_event(_sqs_body(ts="2024-01-01T12:00:00Z"))
    # Sanity-check we converted to ms (anything > 1.7e12 means post-2024).
    assert events[0].last_modified_ms > 1_700_000_000_000


def test_s3_sqs_backend_processes_and_acks():
    delivered = [(b"r1", _sqs_body(key="k1")), (b"r2", _sqs_body(key="k2"))]
    acked: list[bytes] = []

    def receive():
        out = list(delivered)
        delivered.clear()
        return out

    def delete(r):
        acked.append(r)

    backend = S3SqsBackend(client=(receive, delete))
    out = list(backend.poll())
    assert {e.key for e in out} == {"k1", "k2"}
    assert acked == [b"r1", b"r2"]
