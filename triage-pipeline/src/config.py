from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)


@lru_cache(maxsize=1)
def load() -> dict[str, Any]:
    path = Path(os.environ.get("TRIAGE_CONFIG", ROOT / "config" / "pipeline.yaml"))
    with path.open() as fh:
        return yaml.safe_load(fh)


def tenant(tenant_id: str) -> dict[str, Any]:
    for t in load()["tenants"]:
        if t["id"] == tenant_id:
            return t
    raise KeyError(f"unknown tenant: {tenant_id}")


def tenant_ids() -> list[str]:
    return [t["id"] for t in load()["tenants"]]
