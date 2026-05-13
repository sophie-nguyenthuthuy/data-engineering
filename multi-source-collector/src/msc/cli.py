"""``mscctl`` command-line interface."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def cmd_info(_args: argparse.Namespace) -> int:
    from msc import __version__

    print(f"multi-source-collector {__version__}")
    return 0


def cmd_naming(args: argparse.Namespace) -> int:
    from msc.naming import NamingConvention

    key = NamingConvention().make(source=args.source, dataset=args.dataset)
    print(f"path     = {key.path()}")
    print(f"source   = {key.source}")
    print(f"dataset  = {key.dataset}")
    print(f"run_id   = {key.run_id}")
    return 0


def cmd_ingest_csv(args: argparse.Namespace) -> int:
    from msc.manifest import Manifest
    from msc.runner import Runner
    from msc.sources.csv_src import CSVSource
    from msc.staging.zone import StagingZone

    src = CSVSource(path=Path(args.path), dataset=args.dataset, id_column=args.id_column)
    runner = Runner(
        zone=StagingZone(root=Path(args.staging)),
        manifest=Manifest(path=Path(args.manifest)),
    )
    result = runner.ingest(src, run_id=args.run_id)
    print(json.dumps(result.__dict__, sort_keys=True, indent=2))
    return 0


def cmd_list_staging(args: argparse.Namespace) -> int:
    from msc.staging.zone import StagingZone

    zone = StagingZone(root=Path(args.staging))
    for p in zone.list_paths():
        print(p)
    return 0


def cmd_manifest(args: argparse.Namespace) -> int:
    from msc.manifest import Manifest

    m = Manifest(path=Path(args.manifest))
    entries = m.entries()
    print(f"entries: {len(entries)}")
    for e in entries[-args.tail :]:
        print(json.dumps(e.__dict__, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="mscctl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)

    n = sub.add_parser("naming", help="show the staged path for a (source, dataset)")
    n.add_argument("--source", required=True)
    n.add_argument("--dataset", required=True)
    n.set_defaults(func=cmd_naming)

    ic = sub.add_parser("ingest-csv", help="ingest a local CSV into the staging zone")
    ic.add_argument("--path", required=True)
    ic.add_argument("--dataset", required=True)
    ic.add_argument("--id-column", dest="id_column", default=None)
    ic.add_argument("--staging", required=True)
    ic.add_argument("--manifest", required=True)
    ic.add_argument("--run-id", dest="run_id", default=None)
    ic.set_defaults(func=cmd_ingest_csv)

    ls = sub.add_parser("list-staging", help="list every relative path in the staging zone")
    ls.add_argument("--staging", required=True)
    ls.set_defaults(func=cmd_list_staging)

    mf = sub.add_parser("manifest", help="dump the most recent manifest entries")
    mf.add_argument("--manifest", required=True)
    mf.add_argument("--tail", type=int, default=10)
    mf.set_defaults(func=cmd_manifest)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
