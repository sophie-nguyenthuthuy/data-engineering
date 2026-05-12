import random

from producer.events import make_event


def test_make_event_shape():
    rng = random.Random(42)
    ev = make_event(error_rate=0.0, rng=rng)
    assert set(ev) == {
        "event_id", "occurred_at", "user_id", "session_id",
        "event_type", "status", "error_code", "latency_ms",
        "country", "device", "metadata",
    }
    assert ev["status"] == "success"
    assert ev["error_code"] is None
    assert ev["latency_ms"] > 0


def test_error_rate_is_respected():
    rng = random.Random(0)
    errors = sum(make_event(error_rate=1.0, rng=rng)["status"] == "error" for _ in range(200))
    assert errors == 200


def test_error_codes_only_on_errors():
    rng = random.Random(1)
    for _ in range(200):
        ev = make_event(error_rate=0.5, rng=rng)
        if ev["status"] == "error":
            assert ev["error_code"] is not None
        else:
            assert ev["error_code"] is None
