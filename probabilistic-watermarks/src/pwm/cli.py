"""CLI."""

from __future__ import annotations

import argparse
import sys


def cmd_bench(args: argparse.Namespace) -> int:
    if args.what == "throughput":
        from benchmarks.bench_throughput import main as bench_main
        bench_main()
    elif args.what == "calibration":
        from benchmarks.bench_calibration import main as bench_main
        bench_main()
    else:
        print(f"unknown bench target: {args.what}", file=sys.stderr)
        return 2
    return 0


def cmd_info(_: argparse.Namespace) -> int:
    from pwm import __version__
    print(f"probabilistic-watermarks version {__version__}")
    print("Engine: per-key (1-δ)-quantile watermark + correction stream")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pwmctl", description="Probabilistic watermarks CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_bench = sub.add_parser("bench")
    p_bench.add_argument("what", choices=["throughput", "calibration"])
    p_bench.set_defaults(func=cmd_bench)
    p_info = sub.add_parser("info")
    p_info.set_defaults(func=cmd_info)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
