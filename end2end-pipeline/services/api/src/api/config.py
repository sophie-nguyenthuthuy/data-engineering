from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    ch_host: str
    ch_port: int
    ch_db: str
    ch_user: str
    ch_password: str
    ch_table: str

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            ch_host=_require("CLICKHOUSE_HOST"),
            ch_port=int(os.environ.get("CLICKHOUSE_PORT_HTTP", "8123")),
            ch_db=_require("CLICKHOUSE_DB"),
            ch_user=_require("CLICKHOUSE_USER"),
            ch_password=_require_secret("CLICKHOUSE_PASSWORD", "CLICKHOUSE_PASSWORD_FILE"),
            ch_table=os.environ.get("CLICKHOUSE_TABLE", "user_interactions"),
        )


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing required env var: {name}")
    return value


def _require_secret(env_name: str, file_env: str) -> str:
    """Return a secret from ``<env_name>_FILE`` (preferred) or ``<env_name>``.

    The file form is preferred so secrets don't appear in ``docker inspect``
    output. Either form is acceptable; at least one must be set.
    """
    path = os.environ.get(file_env)
    if path:
        try:
            return Path(path).read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            raise RuntimeError(f"{file_env} points at missing file: {path}") from None
    value = os.environ.get(env_name)
    if not value:
        raise RuntimeError(f"missing required secret: set {env_name} or {file_env}")
    return value
