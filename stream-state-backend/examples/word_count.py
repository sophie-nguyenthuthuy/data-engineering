"""
Word-count stream simulation using ValueState.

Simulates a stream of sentences and maintains a per-word count in
ValueState backed by the MemoryBackend.  At the end it prints the
top-10 words by count.
"""

from __future__ import annotations

import random
import sys
import os

# Allow running without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ssb import StateBackendManager, OperatorDescriptor, TopologyDescriptor

# ---------------------------------------------------------------------------
# Sample corpus
# ---------------------------------------------------------------------------

SENTENCES = [
    "the quick brown fox jumps over the lazy dog",
    "to be or not to be that is the question",
    "all that glitters is not gold",
    "the world is a stage and all the men and women merely players",
    "ask not what your country can do for you",
    "one small step for man one giant leap for mankind",
    "the only thing we have to fear is fear itself",
    "in the beginning was the word and the word was with god",
    "it was the best of times it was the worst of times",
    "to be is to be the value of a variable",
]


def process_stream(manager: StateBackendManager, num_events: int = 500) -> None:
    print(f"Processing {num_events} events...")
    for _ in range(num_events):
        sentence = random.choice(SENTENCES)
        for word in sentence.split():
            word = word.lower().strip(".,?!")
            ctx = manager.get_state_context("word_count", word)
            count_state = ctx.get_value_state("count", default=0)
            count_state.set(count_state.get() + 1)


def print_top_counts(manager: StateBackendManager, top_n: int = 10) -> None:
    from ssb.state.serializer import decode_key, decode_value, is_tombstone

    cf = "word_count::count"
    counts: dict[str, int] = {}
    for raw_k, raw_v in manager.backend.scan(cf):
        if is_tombstone(raw_v):
            continue
        try:
            _, value = decode_value(raw_v)
            word = decode_key(raw_k)
            counts[word] = value
        except Exception:
            continue

    sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    print(f"\nTop {top_n} words:")
    print(f"{'Word':<20} {'Count':>8}")
    print("-" * 30)
    for word, count in sorted_counts[:top_n]:
        print(f"{word:<20} {count:>8}")


def main() -> None:
    print("=== Word Count Stream Example ===\n")

    manager = StateBackendManager(backend="memory")
    manager.start()

    # Register topology
    topo = TopologyDescriptor(
        version=1,
        operators={
            "word_count": OperatorDescriptor(
                operator_id="word_count",
                state_names=["count"],
                parallelism=1,
            )
        },
    )
    manager.set_topology(topo)

    process_stream(manager, num_events=1000)
    print_top_counts(manager, top_n=10)

    # Demonstrate state lookup via the API app
    from ssb.api.server import create_app
    from fastapi.testclient import TestClient

    app = create_app(manager)
    client = TestClient(app)

    print("\n--- API Demo ---")
    # List operators
    resp = client.get("/operators")
    print(f"Operators: {resp.json()}")

    # Fetch count for "the"
    resp = client.get('/operators/word_count/count/"the"')
    if resp.status_code == 200:
        print(f"Count for 'the': {resp.json()['value']}")

    # Health check
    resp = client.get("/health")
    print(f"Health: {resp.json()}")

    manager.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
