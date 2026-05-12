"""Single end-to-end smoke test: seed → process → eval, assert rows land."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import auth, ingest, worker  # noqa: E402
from src.eval import harness  # noqa: E402
from src.stubs import pubsub, warehouse  # noqa: E402


def test_end_to_end():
    pubsub.init()
    warehouse.init()
    auth.init()

    ing = ingest.run_once(count_per_tenant=6)
    assert ing["published"] >= 6, ing

    proc = worker.drain()
    assert proc["ok"] >= 1, proc
    # Poison pills should eventually hit DLQ (not guaranteed per run given 8% rate)

    total = warehouse.query("SELECT COUNT(*) AS n FROM emails_processed")[0]["n"]
    assert total >= 1

    ev = harness.run()
    assert 0.0 <= ev["macro_f1"] <= 1.0
    assert len(ev["per_label"]) == 4


if __name__ == "__main__":
    test_end_to_end()
    print("smoke ok")
