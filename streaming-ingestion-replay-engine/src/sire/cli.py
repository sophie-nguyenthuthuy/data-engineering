"""``sirectl`` command-line interface."""

from __future__ import annotations

import argparse


def cmd_info(_args: argparse.Namespace) -> int:
    from sire import __version__

    print(f"streaming-ingestion-replay-engine {__version__}")
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from sire.log.topic import Topic
    from sire.replay import ReplayEngine
    from sire.sinks.collect import CollectingSink

    topic = Topic(name="demo", segment_size_records=args.segment_size)
    for i in range(args.records):
        topic.append(key=f"k{i}".encode(), value=f"v{i}".encode(), timestamp=1000 + i)

    sink = CollectingSink()
    n = ReplayEngine(topic=topic, sink=sink).from_offset(args.from_offset)
    print(f"replayed {n} records starting at offset {args.from_offset}")
    if sink.records:
        first, last = sink.records[0], sink.records[-1]
        print(f"first: offset={first.offset} ts={first.timestamp}")
        print(f"last:  offset={last.offset} ts={last.timestamp}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="sirectl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)
    d = sub.add_parser("demo", help="produce N records then replay from an offset")
    d.add_argument("--records", type=int, default=10)
    d.add_argument("--segment-size", dest="segment_size", type=int, default=4)
    d.add_argument("--from-offset", dest="from_offset", type=int, default=0)
    d.set_defaults(func=cmd_demo)
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
