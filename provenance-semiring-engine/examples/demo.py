"""End-to-end demo: a small pipeline traced through all 4 semirings.

Query: "items bought by customers in city X, with their join provenance"

Tables:
    customers(cid, city)       -- c1: (1, "Hanoi"), c2: (2, "HCMC")
    orders(oid, cid, amount)   -- o1: (10, 1, 50), o2: (11, 1, 70), o3: (12, 2, 30)
"""
from __future__ import annotations

from src import (
    BagSemiring, WhyProvenance, HowProvenance, TriCS,
    annotate, select, join, project, lineage, witness_count,
)


def run(K, label):
    print(f"\n=== {label} ===")
    customers_raw = [(1, "Hanoi"), (2, "HCMC")]
    orders_raw    = [(10, 1, 50), (11, 1, 70), (12, 2, 30)]

    def tok(prefix):
        return lambda i, t: (
            K.singleton(f"{prefix}{i+1}") if hasattr(K, "singleton")
            else (0.8 if i % 2 == 0 else 0.6) if isinstance(K, TriCS)
            else 1  # bag
        )

    customers = annotate(customers_raw, tok("c"), K)
    orders    = annotate(orders_raw,    tok("o"), K)

    # σ_{city=Hanoi}
    cust_hn = select(customers, lambda t: t[1] == "Hanoi", K)
    # join customers ⋈ orders on cid
    joined  = join(cust_hn, orders, key_a=(0,), key_b=(1,), K=K)
    # π_{amount}
    result  = project(joined, (4,), K)   # amount is index 4 in (cid,city,oid,cid',amount)

    for tup, ann in result.items():
        print(f"  amount={tup[0]}  annotation={ann}")
        if isinstance(K, HowProvenance):
            print(f"     lineage={lineage(ann)}  witnesses={witness_count(ann)}")


if __name__ == "__main__":
    run(BagSemiring(),     "Bag semiring (counting)")
    run(WhyProvenance(),   "Why-provenance (witness sets)")
    run(HowProvenance(),   "How-provenance (polynomials)")
    run(TriCS(),           "TriCS (probabilistic)")
