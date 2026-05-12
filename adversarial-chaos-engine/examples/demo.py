"""Demo: register a buggy pipeline; chaos engine surfaces violations."""
from __future__ import annotations

from src import invariant, Runner, emit_pytest


# A "real-world" buggy pipeline: drops rows where amount is negative.
# Looks innocent — but violates row_count_preserved AND sum_invariant.
@invariant(row_count_preserved=True, sum_invariant=["amount"])
def clean_transactions(df):
    return [r for r in df if isinstance(r.get("amount"), (int, float)) and r["amount"] >= 0]


def main():
    print("Running adversarial chaos engine against clean_transactions()...\n")
    r = Runner(seed=42)
    violations = r.run_all(trials_per_fn=50)

    print(f"Discovered {len(violations)} violations.\n")
    # Group by invariant
    from collections import Counter
    inv_counts = Counter(v.invariant for v in violations)
    for inv, cnt in inv_counts.most_common():
        print(f"  {inv}: {cnt}")

    print("\n=== Sample regression test ===\n")
    if violations:
        print(emit_pytest(violations[0]))


if __name__ == "__main__":
    main()
