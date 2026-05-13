"""``cfsctl`` command-line interface."""

from __future__ import annotations

import argparse


def cmd_info(_args: argparse.Namespace) -> int:
    from cfs import __version__

    print(f"causal-feature-store {__version__}")
    return 0


def cmd_demo(_args: argparse.Namespace) -> int:
    from cfs.serving.resolver import Resolver
    from cfs.store.cold import ColdStore
    from cfs.store.hot import HotStore
    from cfs.writer import Writer

    hot, cold = HotStore(k=4), ColdStore()
    w = Writer(hot=hot, cold=cold)
    w.write("u1", "clicks", "n_clicks", 1, wall=10.0)
    w.write("u1", "identity", "is_premium", True, wall=12.0)
    w.write("u1", "clicks", "n_clicks", 2, wall=15.0)

    r = Resolver(hot=hot, cold=cold)
    rv = r.get("u1", ["n_clicks", "is_premium", "missing_feature"])
    print(f"features      = {rv.features}")
    print(f"chosen_clock  = {rv.chosen_clock}")
    print(f"missing       = {rv.missing}")
    print(f"verified      = {r.verify('u1', rv)}")
    return 0


def cmd_partition(_args: argparse.Namespace) -> int:
    from cfs.partition import PartitionScenario

    sc = PartitionScenario()
    sc.write_on("a", "u1", "compA", "f1", "A1", wall=1.0)
    sc.write_on("b", "u1", "compB", "f1", "B1", wall=2.0)
    pre_heal = sc.get("u1", ["f1"])
    sc.heal()
    sc.write_on("a", "u1", "compB", "f2", "joint", wall=3.0)
    post_heal = sc.get("u1", ["f1", "f2"])

    def _fmt(name: str, rv: object) -> None:
        from cfs.serving.resolver import ResolvedVector

        assert isinstance(rv, ResolvedVector)
        print(
            f"[{name}] features={rv.features} chosen_clock={rv.chosen_clock} missing={rv.missing}"
        )

    _fmt("pre-heal", pre_heal)
    _fmt("post-heal", post_heal)
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="cfsctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)
    sub.add_parser("demo", help="write + resolve a small example").set_defaults(func=cmd_demo)
    sub.add_parser("partition", help="simulate a network partition and heal").set_defaults(
        func=cmd_partition
    )
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
