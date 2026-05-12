"""Background scheduler — Kestra-shaped DAG in config/pipeline.yaml.
Runs ingest on ingest_interval_seconds, processing on worker_poll_seconds.
Started by run.py in a daemon thread.
"""
from __future__ import annotations

import threading
import time
import traceback

from .. import ingest, worker
from ..config import load


def _ingest_loop(stop: threading.Event) -> None:
    interval = load()["scheduling"]["ingest_interval_seconds"]
    while not stop.is_set():
        try:
            ingest.run_once(count_per_tenant=3)
        except Exception:
            traceback.print_exc()
        stop.wait(interval)


def _worker_loop(stop: threading.Event) -> None:
    interval = load()["scheduling"]["worker_poll_seconds"]
    while not stop.is_set():
        try:
            worker.drain(max_iters=50)
        except Exception:
            traceback.print_exc()
        stop.wait(interval)


def start() -> threading.Event:
    stop = threading.Event()
    threading.Thread(target=_ingest_loop, args=(stop,), daemon=True, name="ingest").start()
    threading.Thread(target=_worker_loop, args=(stop,), daemon=True, name="worker").start()
    return stop
