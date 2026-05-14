"""Run the same CDC workload against all three formats + compare.

We instrument each format with a small, format-neutral driver and
measure:

  * **commits_or_log_entries** — number of log/timeline/snapshot entries
    appended. Delta + Iceberg commit per *operation*; Hudi MoR can
    batch many deltas before a compaction.
  * **write_amplification** — number of physical files written across
    the whole workload. CoW pays heavily on UPDATE; MoR is cheap.
  * **read_files_at_end** — number of physical files a reader has to
    open to materialise the final state. CoW = 1/group; MoR = 1/group
    + N logs until next compaction.

The same workload runs on each format so the metrics are directly
comparable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from tfl.delta.action import Action, ActionType, FileEntry
from tfl.delta.table import DeltaTable
from tfl.hudi.table import HudiCoWTable, HudiMoRTable
from tfl.iceberg.table import IcebergTable

if TYPE_CHECKING:
    from tfl.bench.workload import Workload


@dataclass(frozen=True, slots=True)
class FormatMetrics:
    """One row of the cross-format comparison table."""

    name: str
    commits: int
    write_amplification: int
    read_files_at_end: int


@dataclass(frozen=True, slots=True)
class CompareReport:
    """Aggregate metrics across formats for one workload."""

    workload: str
    metrics: tuple[FormatMetrics, ...]

    def by_name(self) -> dict[str, FormatMetrics]:
        return {m.name: m for m in self.metrics}

    def lowest_write_amplification(self) -> str:
        return min(self.metrics, key=lambda m: m.write_amplification).name


def _delta_replay(wl: Workload) -> FormatMetrics:
    """Drive Delta with one commit per event."""
    tbl = DeltaTable()
    expected = -1
    expected = tbl.commit([Action(ActionType.METADATA, schema_id=0)], expected_version=expected)
    for e in wl.events:
        if e.op.value == "insert":
            path = f"delta/{e.key}-{expected + 1}.parquet"
            expected = tbl.commit(
                [
                    Action(
                        ActionType.ADD,
                        file=FileEntry(path=path, size_bytes=e.payload_size, record_count=1),
                    )
                ],
                expected_version=expected,
            )
        elif e.op.value == "update":
            # Remove the most recent ADD for that key, add a new file.
            old = next(
                (f for f in reversed(tbl.files_at()) if f.path.startswith(f"delta/{e.key}-")),
                None,
            )
            actions = []
            if old is not None:
                actions.append(Action(ActionType.REMOVE, file=old))
            new_path = f"delta/{e.key}-{expected + 1}.parquet"
            actions.append(
                Action(
                    ActionType.ADD,
                    file=FileEntry(path=new_path, size_bytes=e.payload_size, record_count=1),
                )
            )
            expected = tbl.commit(actions, expected_version=expected)
        else:  # delete
            old = next(
                (f for f in reversed(tbl.files_at()) if f.path.startswith(f"delta/{e.key}-")),
                None,
            )
            if old is not None:
                expected = tbl.commit(
                    [Action(ActionType.REMOVE, file=old)], expected_version=expected
                )
    return FormatMetrics(
        name="delta",
        commits=tbl.n_log_entries(),
        write_amplification=sum(
            1
            for _, actions in [(v, a) for v, a in [(v, a) for v, a in tbl._entries]]
            for a in actions
            if a.type is ActionType.ADD
        ),
        read_files_at_end=len(tbl.files_at()),
    )


def _iceberg_replay(wl: Workload) -> FormatMetrics:
    tbl = IcebergTable()
    n_writes = 0
    for e in wl.events:
        if e.op.value == "insert":
            tbl.append(
                [
                    FileEntry(
                        path=f"iceberg/{e.key}-{tbl.current() or 0}.parquet",
                        size_bytes=e.payload_size,
                        record_count=1,
                    )
                ]
            )
            n_writes += 1
        elif e.op.value == "update":
            old = next(
                (f for f in reversed(tbl.files_at()) if f.path.startswith(f"iceberg/{e.key}-")),
                None,
            )
            new_path = f"iceberg/{e.key}-{tbl.current() or 0}.parquet"
            new_file = FileEntry(path=new_path, size_bytes=e.payload_size, record_count=1)
            if old is not None:
                tbl.delete([old])
            tbl.append([new_file])
            n_writes += 1
        else:
            old = next(
                (f for f in reversed(tbl.files_at()) if f.path.startswith(f"iceberg/{e.key}-")),
                None,
            )
            if old is not None:
                tbl.delete([old])
    return FormatMetrics(
        name="iceberg",
        commits=len(tbl.snapshots()),
        write_amplification=n_writes,
        read_files_at_end=len(tbl.files_at()),
    )


def _hudi_cow_replay(wl: Workload) -> FormatMetrics:
    tbl = HudiCoWTable()
    for e in wl.events:
        if e.op is e.op.DELETE:
            continue
        tbl.upsert(e.key, f"cow/{e.key}-v{tbl.write_amplification() + 1}.parquet")
    return FormatMetrics(
        name="hudi_cow",
        commits=len(tbl.timeline()),
        write_amplification=tbl.write_amplification(),
        read_files_at_end=len(tbl.files()),
    )


def _hudi_mor_replay(wl: Workload, *, compact_every: int = 50) -> FormatMetrics:
    tbl = HudiMoRTable()
    counter = 0
    for e in wl.events:
        if e.op.value == "insert":
            tbl.insert_base(e.key, f"mor/{e.key}-base.parquet")
        elif e.op.value == "update":
            base, _ = tbl.read_paths(e.key)
            if base is None:
                tbl.insert_base(e.key, f"mor/{e.key}-base.parquet")
            tbl.append_log(e.key, f"mor/{e.key}-log-{counter}.avro")
            counter += 1
            if counter % compact_every == 0:
                # Trigger a compaction on each group that has logs.
                for g in list(tbl.all_groups()):
                    _, logs = tbl.read_paths(g)
                    if logs:
                        tbl.compact(g, f"mor/{g}-base-c{counter}.parquet")
    # Read-time file count = base + remaining logs across all groups.
    read_files = sum(1 + len(tbl.read_paths(g)[1]) for g in tbl.all_groups())
    return FormatMetrics(
        name="hudi_mor",
        commits=len(tbl.timeline()),
        write_amplification=tbl.write_amplification(),
        read_files_at_end=read_files,
    )


def run_workload(wl: Workload) -> CompareReport:
    """Drive every format through the same workload and return the metrics."""
    return CompareReport(
        workload=wl.name,
        metrics=(
            _delta_replay(wl),
            _iceberg_replay(wl),
            _hudi_cow_replay(wl),
            _hudi_mor_replay(wl),
        ),
    )


__all__ = ["CompareReport", "FormatMetrics", "run_workload"]
