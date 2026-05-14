"""``lcdcctl`` command-line interface."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def cmd_info(_args: argparse.Namespace) -> int:
    from lcdc import __version__

    print(f"log-based-cdc-from-scratch {__version__}")
    return 0


def cmd_parse_mysql(args: argparse.Namespace) -> int:
    import io

    from lcdc.mysql.reader import BinlogReader

    raw = Path(args.file).read_bytes() if args.file else sys.stdin.buffer.read()
    reader = BinlogReader(stream=io.BytesIO(raw))
    n = 0
    for header, event in reader:
        n += 1
        print(
            f"#{n} type=0x{header.event_type:02x} size={header.event_size} "
            f"pos={header.log_pos} → {type(event).__name__}"
        )
    print(f"events_total = {n}")
    return 0


def cmd_parse_pg(args: argparse.Namespace) -> int:
    """Decode one pgoutput payload from --hex or a binary file."""
    from lcdc.postgres.reader import PgOutputReader

    if args.hex:
        payload = bytes.fromhex(args.hex)
    elif args.file:
        payload = Path(args.file).read_bytes()
    else:
        payload = sys.stdin.buffer.read()
    msg = PgOutputReader.decode(payload)
    print(f"kind = {type(msg).__name__}")
    print(f"repr = {msg}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="lcdcctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)

    m = sub.add_parser("parse-mysql", help="decode a MySQL binlog file")
    m.add_argument("--file", default=None)
    m.set_defaults(func=cmd_parse_mysql)

    g = sub.add_parser("parse-pg", help="decode one pgoutput payload")
    g.add_argument("--hex", default=None, help="payload as hex string")
    g.add_argument("--file", default=None, help="binary file with one payload")
    g.set_defaults(func=cmd_parse_pg)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
