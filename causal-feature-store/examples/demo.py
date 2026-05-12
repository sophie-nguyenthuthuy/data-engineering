"""Demo: write features from multiple components, read back consistent snapshot."""
from __future__ import annotations

from src import HotStore, ColdStore, Writer, Resolver


def main():
    hot = HotStore(k=5)
    cold = ColdStore()
    w = Writer(hot=hot, cold=cold)
    r = Resolver(hot=hot, cold=cold)

    # Simulate events
    w.write("u42", "click_stream",  "click_count",     1)
    w.write("u42", "click_stream",  "click_count",     5)
    w.write("u42", "page_view",     "page_count",      3)
    w.write("u42", "page_view",     "last_page",       "/checkout")
    w.write("u42", "identity",      "is_logged_in",    True)
    w.write("u42", "click_stream",  "click_count",     12)
    w.write("u42", "page_view",     "page_count",      8)

    # Snapshot read
    rv = r.get("u42", [
        "click_count", "page_count", "last_page", "is_logged_in",
        "missing_feature",
    ])
    print("Resolved snapshot:")
    for k, v in rv.features.items():
        print(f"  {k:<20} = {v}")
    print(f"  (missing: {rv.missing})")
    print(f"\nChosen clock: {rv.chosen_clock}")
    print(f"Entity clock: {hot.entity_clock('u42')}")
    print("\nFor every feature, the returned value's write-clock ≤ chosen_clock.")
    print("This is the causal-consistency invariant.")


if __name__ == "__main__":
    main()
