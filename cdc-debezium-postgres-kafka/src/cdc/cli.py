"""``cdcctl`` command-line interface."""

from __future__ import annotations

import argparse
import json
import sys


def cmd_info(_args: argparse.Namespace) -> int:
    from cdc import __version__

    print(f"cdc-debezium-postgres-kafka {__version__}")
    return 0


def cmd_parse(args: argparse.Namespace) -> int:
    from cdc.events.parse import ParseError, parse_envelope

    data = _read_input(args)
    try:
        env = parse_envelope(data)
    except ParseError as exc:
        print(f"parse error: {exc}", file=sys.stderr)
        return 2
    print(f"op       = {env.op.value}")
    print(f"source   = {env.source.db}.{env.source.schema}.{env.source.table}")
    print(f"ts_ms    = {env.ts_ms}")
    print(f"before   = {env.before}")
    print(f"after    = {env.after}")
    return 0


def cmd_schemagen(args: argparse.Namespace) -> int:
    from cdc.schema.avro import generate_avro_schema

    cols: list[tuple[str, str, bool]] = []
    for spec in args.column:
        parts = spec.split(":")
        if len(parts) not in (2, 3):
            raise SystemExit(f"bad --column spec {spec!r}; expected name:pg_type[:nullable]")
        col, pg_type = parts[0], parts[1]
        nullable = len(parts) == 3 and parts[2].lower() in {"1", "true", "yes", "nullable"}
        cols.append((col, pg_type, nullable))
    schema = generate_avro_schema(namespace=args.namespace, name=args.name, columns=cols)
    print(json.dumps(schema, indent=2))
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    from cdc.dlq.router import DLQDecision, DLQRouter

    router = DLQRouter()
    data = _read_input(args)
    decision = router.route(data)
    if isinstance(decision, DLQDecision):
        print(f"DLQ reason={decision.reason.value} message={decision.message}")
        return 1
    print(f"OK op={decision.op.value} {decision.source.table}")
    return 0


def _read_input(args: argparse.Namespace) -> bytes:
    if args.file:
        with open(args.file, "rb") as fh:
            return fh.read()
    return sys.stdin.buffer.read()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="cdcctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)

    pa = sub.add_parser("parse", help="parse one Debezium event from stdin or --file")
    pa.add_argument("--file", default=None)
    pa.set_defaults(func=cmd_parse)

    va = sub.add_parser("validate", help="route one event through the DLQ classifier")
    va.add_argument("--file", default=None)
    va.set_defaults(func=cmd_validate)

    sg = sub.add_parser("schemagen", help="generate an Avro schema from Postgres column specs")
    sg.add_argument("--namespace", required=True)
    sg.add_argument("--name", required=True)
    sg.add_argument(
        "--column",
        action="append",
        default=[],
        help="column spec: name:pg_type[:nullable]",
    )
    sg.set_defaults(func=cmd_schemagen)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
