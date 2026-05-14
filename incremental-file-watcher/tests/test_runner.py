"""End-to-end runner tests + Hypothesis property."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from ifw.backends.inmemory import InMemoryBackend
from ifw.events import FileEvent
from ifw.late import LateArrivalDetector
from ifw.manifest import Manifest
from ifw.runner import Runner


def _evt(key="k", lm=1_000, etag="e1"):
    return FileEvent(bucket="b", key=key, size=1, last_modified_ms=lm, etag=etag)


def _runner(tmp_path, **over):
    backend = over.pop("backend", InMemoryBackend())
    manifest = over.pop("manifest", Manifest(path=tmp_path / "mf.jsonl"))
    processor = over.pop("processor", lambda _e: None)
    return Runner(backend=backend, manifest=manifest, processor=processor, **over)


def test_runner_processes_new_events(tmp_path):
    backend = InMemoryBackend(events=[_evt(key="a"), _evt(key="b", etag="e2")])
    processed: list[FileEvent] = []
    r = _runner(tmp_path, backend=backend, processor=processed.append)
    report = r.run_once()
    assert report.processed == 2
    assert report.duplicates == 0
    assert {e.key for e in processed} == {"a", "b"}


def test_runner_dedupes_repeat_events(tmp_path):
    e = _evt()
    backend = InMemoryBackend(events=[e, e])
    processed: list[FileEvent] = []
    r = _runner(tmp_path, backend=backend, processor=processed.append)
    report = r.run_once()
    assert report.processed == 1
    assert report.duplicates == 1
    assert processed == [e]


def test_runner_rehydrates_dedupe_from_manifest(tmp_path):
    mf = Manifest(path=tmp_path / "mf.jsonl")
    # First run records one event.
    backend1 = InMemoryBackend(events=[_evt()])
    _runner(tmp_path, manifest=mf, backend=backend1).run_once()
    # Second runner instance must skip the same event.
    backend2 = InMemoryBackend(events=[_evt()])
    report = _runner(tmp_path, manifest=mf, backend=backend2).run_once()
    assert report.duplicates == 1
    assert report.processed == 0


def test_runner_separates_late_events(tmp_path):
    backend = InMemoryBackend(
        events=[
            _evt(key="recent", lm=10_000, etag="e1"),
            _evt(key="late", lm=2_000, etag="e2"),
        ]
    )
    late = LateArrivalDetector(watermark_ms=9_000, grace_ms=1_000)
    r = _runner(tmp_path, backend=backend, late=late)
    report = r.run_once()
    # "recent" is not late; "late" is late.
    assert report.processed == 1
    assert report.late == 1


def test_runner_counts_failures(tmp_path):
    def fail(_e):
        raise RuntimeError("boom")

    backend = InMemoryBackend(events=[_evt()])
    report = _runner(tmp_path, backend=backend, processor=fail).run_once()
    assert report.failures == 1
    assert report.processed == 0


def test_runner_does_not_record_failed_events(tmp_path):
    mf = Manifest(path=tmp_path / "mf.jsonl")

    def fail(_e):
        raise RuntimeError("boom")

    backend = InMemoryBackend(events=[_evt()])
    _runner(tmp_path, manifest=mf, backend=backend, processor=fail).run_once()
    assert mf.entries() == []


# ------------------------------------------------------------- Hypothesis


@settings(max_examples=20, deadline=None)
@given(
    st.lists(
        st.tuples(
            st.text(alphabet="abcd", min_size=1, max_size=4),
            st.integers(0, 10_000),
            st.text(alphabet="01", min_size=1, max_size=4),
        ),
        min_size=0,
        max_size=20,
    )
)
def test_property_runner_idempotent_on_replay(tmp_path_factory, batch):
    """Running the same backend twice never re-processes an event."""
    tmp_path = tmp_path_factory.mktemp("ifw")
    events = [
        FileEvent(bucket="b", key=k, size=0, last_modified_ms=lm, etag=tag)
        for (k, lm, tag) in batch
    ]
    mf = Manifest(path=tmp_path / "mf.jsonl")

    seen_first: list[FileEvent] = []
    seen_second: list[FileEvent] = []
    Runner(
        backend=InMemoryBackend(events=list(events)),
        manifest=mf,
        processor=seen_first.append,
    ).run_once()
    Runner(
        backend=InMemoryBackend(events=list(events)),
        manifest=mf,
        processor=seen_second.append,
    ).run_once()
    assert seen_second == []
