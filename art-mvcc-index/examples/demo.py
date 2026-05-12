"""Demo: ART + MVCC scenarios."""
from __future__ import annotations

import random

from src import ART, MVCCArt


def main():
    print("=== ART: indexing 10k random byte keys ===")
    rng = random.Random(0)
    a = ART()
    for _ in range(10_000):
        k = bytes(rng.randint(0, 255) for _ in range(rng.randint(2, 8)))
        a.put(k, len(k))
    print(f"  Inserted ~10k keys")
    print(f"  Root type: {type(a.root).__name__}")

    print("\n=== MVCC: snapshot isolation ===")
    db = MVCCArt()
    db.put(1, "Alice")
    db.put(2, "Bob")
    snap_v1 = db.begin()
    print(f"  Snapshot v1 (ts={snap_v1.ts}): 1={snap_v1.get(1)}, 2={snap_v1.get(2)}")

    db.put(1, "Alice (updated)")
    db.delete(2)
    db.put(3, "Charlie")
    snap_v2 = db.begin()
    print(f"  Snapshot v2 (ts={snap_v2.ts}):")
    print(f"    1={snap_v2.get(1)}, 2={snap_v2.get(2)}, 3={snap_v2.get(3)}")

    print(f"  Snapshot v1 (still): 1={snap_v1.get(1)}, 2={snap_v1.get(2)}, 3={snap_v1.get(3)}")
    print("  → Long-running analytics readers can hold v1 while writers proceed.")


if __name__ == "__main__":
    main()
