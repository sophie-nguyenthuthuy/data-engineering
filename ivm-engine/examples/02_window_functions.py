"""Example 2 — Window functions (tumbling, sliding, partition ranking).

Scenario: web server request log.

Demonstrates:
  - TumblingWindow: request count per 10-second bucket.
  - SlidingWindow:  rolling 30-second avg latency (step = 10 s).
  - PartitionWindow (ROW_NUMBER): top-N requests per user session.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ivm import IVMEngine, TumblingWindow, SlidingWindow, PartitionWindow
import ivm.aggregates as agg


def main():
    print("=" * 60)
    print("Window functions example — web request log")
    print("=" * 60)

    # ----------------------------------------------------------------
    # 1. Tumbling window — request count per 10-second bucket
    # ----------------------------------------------------------------
    engine = IVMEngine()
    reqs = engine.source("requests")

    tumbling_view = reqs.window(
        TumblingWindow(size_ms=10_000),   # 10 second buckets
        aggregates={
            "request_count": agg.Count(),
            "total_bytes":   agg.Sum("bytes"),
            "avg_latency_ms": agg.Avg("latency_ms"),
        },
    )
    engine.register_view("tumbling", tumbling_view)

    events = [
        # t=2000: window 0 (0–9999 ms)
        {"path": "/home",    "bytes": 1200, "latency_ms": 12},
        {"path": "/api/v1",  "bytes": 350,  "latency_ms": 45},
        # t=8000: still window 0
        {"path": "/home",    "bytes": 1100, "latency_ms": 9},
    ]
    for i, ev in enumerate(events):
        ts = [2000, 5000, 8000][i]
        engine.ingest("requests", ev, timestamp=ts)

    print("\n[Tumbling 10s] After 3 events in window 0:")
    for row in engine.query("tumbling"):
        print(f"  window [{row['window_start']}–{row['window_end']} ms]  "
              f"count={row['request_count']}  bytes={row['total_bytes']}  "
              f"avg_latency={row['avg_latency_ms']:.1f} ms")

    # t=12000: window 1 (10000–19999 ms)
    engine.ingest("requests", {"path": "/login", "bytes": 800, "latency_ms": 88},
                  timestamp=12_000)
    engine.ingest("requests", {"path": "/api/v2", "bytes": 200, "latency_ms": 22},
                  timestamp=15_000)

    print("\n[Tumbling 10s] After 2 more events in window 1:")
    for row in sorted(engine.query("tumbling"), key=lambda r: r["window_start"]):
        print(f"  window [{row['window_start']}–{row['window_end']} ms]  "
              f"count={row['request_count']}  avg_latency={row['avg_latency_ms']:.1f} ms")

    # ----------------------------------------------------------------
    # 2. Sliding window — 30-second window, 10-second step
    # ----------------------------------------------------------------
    print("\n" + "-" * 60)
    engine2 = IVMEngine()
    reqs2 = engine2.source("requests")

    sliding_view = reqs2.window(
        SlidingWindow(size_ms=30_000, step_ms=10_000),
        aggregates={
            "count":       agg.Count(),
            "avg_latency": agg.Avg("latency_ms"),
        },
    )
    engine2.register_view("sliding", sliding_view)

    slide_events = [
        (5_000,  {"path": "/a", "latency_ms": 10}),
        (15_000, {"path": "/b", "latency_ms": 30}),
        (25_000, {"path": "/c", "latency_ms": 50}),
        (35_000, {"path": "/d", "latency_ms": 20}),
    ]
    for ts, ev in slide_events:
        engine2.ingest("requests", ev, timestamp=ts)

    print("\n[Sliding 30s/10s] Windows after 4 events:")
    for row in sorted(engine2.query("sliding"), key=lambda r: r["window_start"]):
        print(f"  [{row['window_start']}–{row['window_end']} ms]  "
              f"count={row['count']}  avg_latency={row['avg_latency']:.1f} ms")

    # ----------------------------------------------------------------
    # 3. PartitionWindow — ROW_NUMBER per user, ordered by latency desc
    # ----------------------------------------------------------------
    print("\n" + "-" * 60)
    engine3 = IVMEngine()
    reqs3 = engine3.source("requests")

    ranked_view = reqs3.window(
        PartitionWindow(
            partition_by=["user_id"],
            order_by=[("latency_ms", "desc")],
        ),
        rank_fns={"rn": "row_number"},
    )
    engine3.register_view("ranked", ranked_view)

    user_events = [
        {"user_id": "alice", "path": "/a", "latency_ms": 100},
        {"user_id": "alice", "path": "/b", "latency_ms": 50},
        {"user_id": "bob",   "path": "/c", "latency_ms": 200},
        {"user_id": "alice", "path": "/d", "latency_ms": 75},
        {"user_id": "bob",   "path": "/e", "latency_ms": 10},
    ]
    for ev in user_events:
        engine3.ingest("requests", ev, timestamp=1000)

    print("\n[PartitionWindow ROW_NUMBER by latency desc] Rankings:")
    rows = sorted(engine3.query("ranked"), key=lambda r: (r["user_id"], r["rn"]))
    for r in rows:
        print(f"  user={r['user_id']}  rn={r['rn']}  "
              f"path={r['path']}  latency={r['latency_ms']} ms")

    # Retract the slowest alice request — ranks should shift
    print("\nRetracting alice /a (latency=100)…")
    engine3.retract("requests",
                    {"user_id": "alice", "path": "/a", "latency_ms": 100},
                    timestamp=2000)
    print("Rankings after retraction:")
    rows = sorted(engine3.query("ranked"), key=lambda r: (r["user_id"], r["rn"]))
    for r in rows:
        print(f"  user={r['user_id']}  rn={r['rn']}  "
              f"path={r['path']}  latency={r['latency_ms']} ms")


if __name__ == "__main__":
    main()
