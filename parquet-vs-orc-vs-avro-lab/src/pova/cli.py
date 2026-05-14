"""``povactl`` command-line interface."""

from __future__ import annotations

import argparse
import random


def cmd_info(_args: argparse.Namespace) -> int:
    from pova import __version__

    print(f"parquet-vs-orc-vs-avro-lab {__version__}")
    return 0


def _gen_columns(n: int, seed: int) -> list:  # type: ignore[type-arg]
    from pova.columnar.column import Column, ColumnType

    rng = random.Random(seed)
    ids = list(range(n))
    cats = [rng.choice(["A", "B", "C", "D"]) for _ in range(n)]
    amts = [round(rng.uniform(0.0, 100.0), 2) for _ in range(n)]
    return [
        Column(name="id", type=ColumnType.INT64, values=tuple(ids)),
        Column(name="category", type=ColumnType.STRING, values=tuple(cats)),
        Column(name="amount", type=ColumnType.FLOAT64, values=tuple(amts)),
    ]


def cmd_bench(args: argparse.Namespace) -> int:
    from pova.bench import run_benchmark
    from pova.columnar.column import ColumnType
    from pova.columnar.schema import Schema

    schema = Schema(
        fields=(
            ("id", ColumnType.INT64),
            ("category", ColumnType.STRING),
            ("amount", ColumnType.FLOAT64),
        )
    )
    columns = _gen_columns(args.rows, args.seed)
    result = run_benchmark(schema, columns)
    print(f"rows={result.n_rows}")
    print(f"{'format':<10} {'bytes':>10} {'write_s':>10} {'read_s':>10}")
    for r in result.results:
        print(
            f"{r.name:<10} {r.bytes_written:>10} {r.write_seconds:>10.4f} {r.read_seconds:>10.4f}"
        )
    print(f"smallest = {result.best_compression()}")
    print(f"fast_read = {result.fastest_read()}")
    print(f"fast_write = {result.fastest_write()}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="povactl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)
    b = sub.add_parser("bench", help="benchmark all three formats on synthetic data")
    b.add_argument("--rows", type=int, default=2_000)
    b.add_argument("--seed", type=int, default=0)
    b.set_defaults(func=cmd_bench)
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
