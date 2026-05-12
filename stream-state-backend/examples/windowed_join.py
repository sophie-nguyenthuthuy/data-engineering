"""
Windowed join simulation using ListState (buffer) + MapState (join index).

Two streams are simulated:
  - Stream A: user click events  {user_id, page, timestamp}
  - Stream B: user purchase events {user_id, item, amount, timestamp}

For each user we maintain:
  - ``click_buffer`` (ListState, TTL=5s)  — recent click events
  - ``purchase_index`` (MapState, TTL=5s) — purchase_id → purchase details
  - ``join_results`` (ListState)          — successfully joined pairs

When a purchase arrives we scan the click buffer and emit a join result
for each click that occurred within the same window.
"""

from __future__ import annotations

import random
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ssb import StateBackendManager, OperatorDescriptor, TopologyDescriptor, TTLConfig

# ---------------------------------------------------------------------------
# Simulated event generators
# ---------------------------------------------------------------------------

PAGES = ["/home", "/products", "/cart", "/checkout", "/about"]
ITEMS = ["widget", "gadget", "doohickey", "thingamajig", "whatchamacallit"]
USER_IDS = [f"user_{i}" for i in range(1, 6)]


def gen_click(ts: float) -> dict:
    return {
        "type": "click",
        "user_id": random.choice(USER_IDS),
        "page": random.choice(PAGES),
        "ts": ts,
    }


def gen_purchase(ts: float) -> dict:
    return {
        "type": "purchase",
        "user_id": random.choice(USER_IDS),
        "item": random.choice(ITEMS),
        "amount": round(random.uniform(5.0, 200.0), 2),
        "purchase_id": f"p_{int(ts * 1000)}_{random.randint(1000, 9999)}",
        "ts": ts,
    }


# ---------------------------------------------------------------------------
# Stream processor
# ---------------------------------------------------------------------------


class WindowedJoinProcessor:
    """Processes interleaved click and purchase events."""

    def __init__(self, manager: StateBackendManager, window_ttl_ms: int = 5000) -> None:
        self._mgr = manager
        self._ttl = TTLConfig(ttl_ms=window_ttl_ms, update_on_read=False)
        self._join_count = 0

    def handle(self, event: dict) -> list[dict]:
        """Process one event and return any join results produced."""
        user_id = event["user_id"]
        ctx = self._mgr.get_state_context("windowed_join", user_id)

        click_buf = ctx.get_list_state("click_buffer", ttl=self._ttl)
        purchase_idx = ctx.get_map_state("purchase_index", ttl=self._ttl)
        join_results = ctx.get_list_state("join_results")

        results: list[dict] = []

        if event["type"] == "click":
            # Buffer this click event
            click_buf.add({"page": event["page"], "ts": event["ts"]})

        elif event["type"] == "purchase":
            # Index the purchase
            purchase_idx.put(event["purchase_id"], {
                "item": event["item"],
                "amount": event["amount"],
                "ts": event["ts"],
            })

            # Join with all buffered clicks
            for click in click_buf.get():
                result = {
                    "user_id": user_id,
                    "click_page": click["page"],
                    "click_ts": click["ts"],
                    "purchase_id": event["purchase_id"],
                    "item": event["item"],
                    "amount": event["amount"],
                    "purchase_ts": event["ts"],
                }
                join_results.add(result)
                results.append(result)
                self._join_count += 1

        return results


def simulate(processor: WindowedJoinProcessor, n_events: int = 50) -> None:
    ts = time.time()
    events = []
    for i in range(n_events):
        ts += random.uniform(0.001, 0.05)
        if random.random() < 0.7:
            events.append(gen_click(ts))
        else:
            events.append(gen_purchase(ts))

    print(f"Simulating {n_events} events...")
    join_outputs = []
    for ev in events:
        results = processor.handle(ev)
        join_outputs.extend(results)

    print(f"  Events processed: {n_events}")
    print(f"  Join results:     {len(join_outputs)}")
    if join_outputs:
        sample = random.choice(join_outputs)
        print(f"  Sample join: user={sample['user_id']} "
              f"page={sample['click_page']} "
              f"item={sample['item']} "
              f"amount=${sample['amount']:.2f}")


def demonstrate_ttl_expiry(manager: StateBackendManager) -> None:
    """Show that buffers expire and are cleaned up by the compactor."""
    print("\n--- TTL Expiry Demo ---")
    short_ttl = TTLConfig(ttl_ms=200)  # 200ms window
    ctx = manager.get_state_context("windowed_join", "demo_user")
    click_buf = ctx.get_list_state("click_buffer", ttl=short_ttl)

    click_buf.add({"page": "/demo", "ts": time.time()})
    print(f"Click buffer before expiry: {click_buf.get()}")

    time.sleep(0.3)
    print(f"Click buffer after 300ms:   {click_buf.get()} (expired, should be [])")

    manager.compactor.run_once()
    from ssb.state.serializer import encode_key
    raw = manager.backend.get("windowed_join::click_buffer", encode_key("demo_user"))
    print(f"Backend entry after compaction: {'None (cleaned up)' if raw is None else 'still present'}")


def main() -> None:
    print("=== Windowed Join Stream Example ===\n")

    manager = StateBackendManager(backend="memory")
    manager.start()

    # Register topology
    topo = TopologyDescriptor(
        version=1,
        operators={
            "windowed_join": OperatorDescriptor(
                operator_id="windowed_join",
                state_names=["click_buffer", "purchase_index", "join_results"],
                parallelism=1,
            )
        },
    )
    manager.set_topology(topo)

    processor = WindowedJoinProcessor(manager, window_ttl_ms=5000)
    simulate(processor, n_events=100)
    demonstrate_ttl_expiry(manager)

    # Query via API
    from ssb.api.server import create_app
    from fastapi.testclient import TestClient

    app = create_app(manager)
    client = TestClient(app)

    print("\n--- API Queries ---")
    resp = client.get("/operators/windowed_join/state-names")
    print(f"State names: {resp.json()}")

    resp = client.get("/operators/windowed_join/join_results/keys")
    data = resp.json()
    print(f"Users with join results: {data['keys']}")

    if data["keys"]:
        import json
        user = data["keys"][0]
        key_param = json.dumps(user)
        resp = client.get(f"/operators/windowed_join/join_results/{key_param}")
        if resp.status_code == 200:
            results = resp.json()["value"]
            print(f"Join results for {user}: {len(results)} matches")

    resp = client.get("/health")
    print(f"Health: {resp.json()}")

    manager.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
