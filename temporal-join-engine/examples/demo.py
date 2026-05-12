"""
Demo: Temporal Join Engine — AS OF joins with late-arrival corrections.

Scenario
--------
Two streams of sensor readings and enrichment events for two devices.
The enrichment stream (right) has a higher lateness tolerance than the
reading stream (left).  One enrichment event arrives late and triggers
a correction for an already-emitted join result.
"""
from temporal_join import AsOfJoinEngine, Event, STREAM_LEFT, STREAM_RIGHT

LOOKBACK  = 30_000   # 30 s: how far back to look for an enrichment event
LEFT_LATE =  5_000   # 5 s  lateness budget for sensor readings
RIGHT_LATE = 15_000  # 15 s lateness budget for enrichment events


def left(device: str, t: int, **kw) -> Event:
    return Event(key=device, event_time=t, stream_id=STREAM_LEFT, payload=kw)


def right(device: str, t: int, **kw) -> Event:
    return Event(key=device, event_time=t, stream_id=STREAM_RIGHT, payload=kw)


def show(tag: str, results):
    for r in results:
        print(f"  [{tag}] {r}")
    if not results:
        print(f"  [{tag}] (no output)")


def main():
    engine = AsOfJoinEngine(
        lookback_window=LOOKBACK,
        left_lateness_bound=LEFT_LATE,
        right_lateness_bound=RIGHT_LATE,
    )

    print("=== Phase 1: enrichment events arrive before readings ===")
    show("R", engine.process_event(right("dev-A", 1_000, firmware="v1.0")))
    show("R", engine.process_event(right("dev-B", 2_000, firmware="v2.0")))

    print("\n=== Phase 2: sensor readings — should match enrichment events ===")
    show("L", engine.process_event(left("dev-A", 5_000, temp=22.1)))   # matches R@1000
    show("L", engine.process_event(left("dev-B", 6_000, temp=19.5)))   # matches R@2000
    show("L", engine.process_event(left("dev-A", 7_000, temp=22.4)))   # matches R@1000

    print("\n=== Phase 3: on-time enrichment advances frontier ===")
    show("R", engine.process_event(right("dev-A", 20_000, firmware="v1.1")))
    # Right watermark = 20000 - 15000 = 5000
    print(f"  Right watermark: {engine.right_watermark}")

    print("\n=== Phase 4: late enrichment for dev-A (T=3500, within lateness window) ===")
    # T=3500 >= watermark=5000? No. Let's push the frontier a bit more first.
    show("R", engine.process_event(right("dev-A", 22_000, firmware="v1.0b")))
    # Right watermark = 22000 - 15000 = 7000; T=3500 < 7000 → irreparably late for this run
    # Use a closer late event to demonstrate corrections:
    # T=3500 won't work here; use T=8000 (within lateness window relative to 22000)
    print("  (sending late enrichment at T=8000 — reclaimably late)")
    corrections = engine.process_event(right("dev-A", 8_000, firmware="v1.0-hotfix"))
    show("LATE-R corrections", corrections)

    print("\n=== Phase 5: new left event after correction ===")
    show("L", engine.process_event(left("dev-A", 25_000, temp=21.9)))

    print("\n=== Phase 6: reading with no match in lookback window ===")
    engine2 = AsOfJoinEngine(lookback_window=500, right_lateness_bound=5_000)
    engine2.process_event(right("dev-C", 100, firmware="old"))
    show("L(no-match)", engine2.process_event(left("dev-C", 700, temp=18.0)))  # 700-100=600 > 500


if __name__ == "__main__":
    main()
