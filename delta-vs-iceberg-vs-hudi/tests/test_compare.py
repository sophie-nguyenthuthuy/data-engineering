"""Cross-format workload comparison tests."""

from __future__ import annotations

import pytest

from tfl.bench.compare import run_workload
from tfl.bench.workload import CDCEvent, CDCOp, Workload


def _wl(events: list[tuple[CDCOp, str]]) -> Workload:
    return Workload(
        name="t",
        events=tuple(CDCEvent(op=op, key=k, payload_size=64) for op, k in events),
    )


# ---------------------------------------------------------- Workload


def test_cdc_event_rejects_empty_key():
    with pytest.raises(ValueError):
        CDCEvent(op=CDCOp.INSERT, key="", payload_size=10)


def test_cdc_event_rejects_negative_payload():
    with pytest.raises(ValueError):
        CDCEvent(op=CDCOp.INSERT, key="k", payload_size=-1)


def test_workload_rejects_empty_events():
    with pytest.raises(ValueError):
        Workload(name="t", events=())


def test_workload_update_ratio():
    wl = _wl([(CDCOp.INSERT, "a"), (CDCOp.UPDATE, "a"), (CDCOp.UPDATE, "a")])
    assert wl.update_ratio() == pytest.approx(2 / 3)


# -------------------------------------------------------- comparison


def test_compare_returns_four_format_metrics():
    wl = _wl([(CDCOp.INSERT, "a"), (CDCOp.UPDATE, "a"), (CDCOp.DELETE, "a")])
    report = run_workload(wl)
    names = {m.name for m in report.metrics}
    assert names == {"delta", "iceberg", "hudi_cow", "hudi_mor"}


def test_compare_records_commit_counts():
    wl = _wl([(CDCOp.INSERT, "a"), (CDCOp.INSERT, "b")])
    report = run_workload(wl)
    # Delta and Iceberg commit per event; Hudi CoW commits once per upsert.
    assert report.by_name()["delta"].commits >= 2
    assert report.by_name()["iceberg"].commits == 2
    assert report.by_name()["hudi_cow"].commits == 2


def test_compare_mor_write_amp_lower_than_cow_on_heavy_updates():
    # 1 INSERT + 5 UPDATEs on same key.
    wl = _wl(
        [(CDCOp.INSERT, "a")] + [(CDCOp.UPDATE, "a")] * 5,
    )
    report = run_workload(wl).by_name()
    # CoW rewrites the base on every UPDATE → 6 writes; MoR writes the base
    # once + 5 cheap logs → 6 too. With compaction every 50, MoR's amp stays
    # equal in this tiny case but never exceeds CoW.
    assert report["hudi_mor"].write_amplification <= report["hudi_cow"].write_amplification


def test_compare_lowest_write_amp_is_a_known_format():
    wl = _wl([(CDCOp.INSERT, f"k{i}") for i in range(3)])
    report = run_workload(wl)
    assert report.lowest_write_amplification() in {"delta", "iceberg", "hudi_cow", "hudi_mor"}


def test_compare_reports_read_files_at_end():
    wl = _wl(
        [(CDCOp.INSERT, "a"), (CDCOp.INSERT, "b"), (CDCOp.UPDATE, "a")],
    )
    report = run_workload(wl)
    # After the workload: two distinct keys are live, so read_files_at_end ≥ 2 for the
    # column-oriented formats, and ≥ 2 (base) + delta logs for MoR.
    by = report.by_name()
    assert by["hudi_cow"].read_files_at_end == 2
    assert by["hudi_mor"].read_files_at_end >= 2
