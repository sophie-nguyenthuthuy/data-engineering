"""Entry point — starts stub infra, seeds some data, launches scheduler + dashboard."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

import uvicorn

from src import auth, ingest, worker  # noqa: E402
from src.config import load  # noqa: E402
from src.eval import harness  # noqa: E402
from src.orchestrator import scheduler  # noqa: E402
from src.stubs import pubsub, warehouse  # noqa: E402


def _bootstrap() -> None:
    pubsub.init()
    warehouse.init()
    auth.init()


def cmd_seed() -> None:
    _bootstrap()
    print(ingest.run_once(count_per_tenant=8))
    print(worker.drain())
    print(harness.run())


def cmd_ingest() -> None:
    _bootstrap()
    print(ingest.run_once())


def cmd_process() -> None:
    _bootstrap()
    print(worker.drain())


def cmd_eval() -> None:
    _bootstrap()
    print(harness.run())


def cmd_serve(no_scheduler: bool = False) -> None:
    _bootstrap()
    cfg = load()["dashboard"]
    if not no_scheduler:
        scheduler.start()
    uvicorn.run("src.dashboard.app:app", host=cfg["host"], port=cfg["port"], log_level="info")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd", choices=["serve", "seed", "ingest", "process", "eval"])
    parser.add_argument("--no-scheduler", action="store_true", help="serve without background scheduler")
    args = parser.parse_args()
    {
        "serve": lambda: cmd_serve(args.no_scheduler),
        "seed": cmd_seed,
        "ingest": cmd_ingest,
        "process": cmd_process,
        "eval": cmd_eval,
    }[args.cmd]()


if __name__ == "__main__":
    main()
