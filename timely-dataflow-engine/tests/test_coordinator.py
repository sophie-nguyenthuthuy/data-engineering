"""Multi-worker progress coordinator."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from timely.progress.coordinator import ProgressCoordinator
from timely.timestamp.ts import Timestamp

if TYPE_CHECKING:
    from timely.timestamp.antichain import Antichain


def test_single_worker_basic():
    coord = ProgressCoordinator()
    coord.worker_update(worker_id=0, op="op", ts=Timestamp(0, 0), delta=+1)
    assert coord.tracker.count("op", Timestamp(0, 0)) == 1
    coord.worker_update(worker_id=0, op="op", ts=Timestamp(0, 0), delta=-1)
    assert coord.tracker.count("op", Timestamp(0, 0)) == 0


def test_multiple_workers_share_state():
    coord = ProgressCoordinator()
    coord.worker_update(0, "op", Timestamp(0, 0), +3)
    coord.worker_update(1, "op", Timestamp(0, 0), +2)
    assert coord.tracker.count("op", Timestamp(0, 0)) == 5


def test_subscribe_advances():
    coord = ProgressCoordinator()
    seen: list[Antichain] = []
    coord.subscribe(lambda ac: seen.append(ac))
    coord.worker_update(0, "op", Timestamp(0, 0), +1)
    assert len(seen) >= 1


def test_advance_count_increments_on_change():
    coord = ProgressCoordinator()
    coord.worker_update(0, "op", Timestamp(0, 0), +1)
    advances_after_first = coord.advances
    coord.worker_update(0, "op", Timestamp(0, 0), -1)
    coord.worker_update(0, "op", Timestamp(1, 0), +1)
    # State changed → frontier changed → advances incremented
    assert coord.advances > advances_after_first


def test_concurrent_workers_no_corruption():
    coord = ProgressCoordinator()
    n_threads = 8
    per_thread = 200

    def worker(wid: int) -> None:
        for i in range(per_thread):
            coord.worker_update(wid, "op", Timestamp(0, i), +1)
            coord.worker_update(wid, "op", Timestamp(0, i), -1)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # All counts should be zero
    assert coord.tracker.total_pending() == 0
