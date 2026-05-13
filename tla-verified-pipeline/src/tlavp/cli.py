"""CLI."""

from __future__ import annotations

import argparse
import json


def cmd_replay(args: argparse.Namespace) -> int:
    from pathlib import Path

    from tlavp.monitor.alerts import ConsoleAlertSink
    from tlavp.monitor.replay import Monitor
    events = json.loads(Path(args.events).read_text())
    mon = Monitor(
        max_lag=args.max_lag,
        max_steps_to_delivery=args.max_steps_to_delivery,
        alert_sink=ConsoleAlertSink(),
    )
    incidents = mon.replay(events)
    print(f"\nReplay complete: {len(incidents)} incident(s)")
    print(f"Final state: pg={len(mon.machine.state.pg)} kafka={len(mon.machine.state.kafka)} "
          f"rev_etl={len(mon.machine.state.rev_etl)}")
    return 0 if not incidents else 1


def cmd_demo(args: argparse.Namespace) -> int:
    from tlavp.monitor.alerts import ConsoleAlertSink
    from tlavp.monitor.replay import Monitor
    from tlavp.workload import buggy_stream, healthy_stream

    mon = Monitor(max_lag=args.max_lag, alert_sink=ConsoleAlertSink())
    if args.scenario == "healthy":
        events = healthy_stream(n_records=args.n_records)
    else:
        events = buggy_stream(args.scenario, n_records=args.n_records)

    incidents = mon.replay(events)
    print(f"\n=== {args.scenario}: {len(incidents)} incidents, "
          f"{len(mon.machine.state.rev_etl)} records delivered ===")
    return 0


def cmd_info(_: argparse.Namespace) -> int:
    from tlavp import __version__
    print(f"tla-verified-pipeline version {__version__}")
    print("Pipeline: PG → Debezium → Kafka → Flink → Warehouse → Reverse-ETL")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tlavpctl",
                                     description="TLA+-verified pipeline monitor")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_replay = sub.add_parser("replay", help="Replay an event log JSON file")
    p_replay.add_argument("events")
    p_replay.add_argument("--max-lag", type=int, default=100)
    p_replay.add_argument("--max-steps-to-delivery", type=int, default=10_000)
    p_replay.set_defaults(func=cmd_replay)

    p_demo = sub.add_parser("demo")
    p_demo.add_argument("scenario", choices=["healthy", "kafka_lag", "lost_publish",
                                              "double_publish"])
    p_demo.add_argument("--n-records", type=int, default=5)
    p_demo.add_argument("--max-lag", type=int, default=10)
    p_demo.set_defaults(func=cmd_demo)

    p_info = sub.add_parser("info")
    p_info.set_defaults(func=cmd_info)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
