"""CLI."""

from __future__ import annotations

import argparse


def cmd_demo(_args: argparse.Namespace) -> int:
    from prov.operators import annotate, join, project, select
    from prov.semiring.how import HowProvenance
    from prov.semiring.why import WhyProvenance

    customers = [(1, "Hanoi"), (2, "HCMC"), (3, "Hanoi")]
    orders = [(10, 1, 50), (11, 1, 70), (12, 2, 30), (13, 3, 25)]

    print("=== How-provenance ===")
    H = HowProvenance()
    cust = annotate(customers, lambda i, _t: H.singleton(f"c{i+1}"), H)
    ord_ = annotate(orders, lambda i, _t: H.singleton(f"o{i+1}"), H)

    hn = select(cust, lambda t: t[1] == "Hanoi", H)
    joined = join(hn, ord_, key_a=(0,), key_b=(1,), K=H)
    result = project(joined, (4,), H)   # amount
    for tup, ann in sorted(result.items()):
        print(f"  amount={tup[0]}  annotation={ann}")

    print("\n=== Why-provenance ===")
    W = WhyProvenance()
    cust_w = annotate(customers, lambda i, _t: W.singleton(f"c{i+1}"), W)
    ord_w = annotate(orders, lambda i, _t: W.singleton(f"o{i+1}"), W)
    hn_w = select(cust_w, lambda t: t[1] == "Hanoi", W)
    joined_w = join(hn_w, ord_w, key_a=(0,), key_b=(1,), K=W)
    result_w = project(joined_w, (4,), W)
    for tup, ann in sorted(result_w.items()):
        print(f"  amount={tup[0]}  witnesses={[set(w) for w in W.witnesses(ann)]}")
    return 0


def cmd_info(_args: argparse.Namespace) -> int:
    from prov import __version__
    print(f"provenance-semiring-engine {__version__}")
    print("Semirings: Bag (N), Boolean, Why, How (polynomials), TriCS (probabilistic)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="provctl")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("demo").set_defaults(func=cmd_demo)
    sub.add_parser("info").set_defaults(func=cmd_info)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
