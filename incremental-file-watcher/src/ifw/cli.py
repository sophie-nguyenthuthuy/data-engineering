"""``ifwctl`` command-line interface."""

from __future__ import annotations

import argparse
from pathlib import Path


def cmd_info(_args: argparse.Namespace) -> int:
    from ifw import __version__

    print(f"incremental-file-watcher {__version__}")
    return 0


def cmd_manifest(args: argparse.Namespace) -> int:
    from ifw.manifest import Manifest

    m = Manifest(path=Path(args.manifest))
    entries = m.entries()
    print(f"entries     = {len(entries)}")
    print(f"watermark   = {m.watermark_ms()} ms")
    for e in entries[-args.tail :]:
        print(f"  {e.bucket}/{e.key}  etag={e.etag}  lm={e.last_modified_ms}")
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from ifw.backends.inmemory import InMemoryBackend
    from ifw.events import FileEvent
    from ifw.manifest import Manifest
    from ifw.runner import Runner

    backend = InMemoryBackend()
    for i in range(args.events):
        backend.push(
            FileEvent(
                bucket="demo",
                key=f"data/file-{i:04d}.parquet",
                size=1024 * (i + 1),
                last_modified_ms=1_000 + i,
                etag=f"etag-{i:04d}",
            )
        )
    processed: list[FileEvent] = []
    runner = Runner(
        backend=backend,
        manifest=Manifest(path=Path(args.manifest)),
        processor=processed.append,
    )
    report = runner.run_once()
    print(f"events_in={args.events}  processed={report.processed}  duplicates={report.duplicates}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="ifwctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)

    mf = sub.add_parser("manifest", help="dump the manifest tail")
    mf.add_argument("--manifest", required=True)
    mf.add_argument("--tail", type=int, default=10)
    mf.set_defaults(func=cmd_manifest)

    d = sub.add_parser("demo", help="ingest a synthetic batch into the manifest")
    d.add_argument("--manifest", required=True)
    d.add_argument("--events", type=int, default=10)
    d.set_defaults(func=cmd_demo)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
