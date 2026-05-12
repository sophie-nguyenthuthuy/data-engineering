from __future__ import annotations

from functools import lru_cache
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver import Client

from .config import Config


@lru_cache(maxsize=1)
def _client_for(cfg: Config) -> Client:
    return clickhouse_connect.get_client(
        host=cfg.ch_host,
        port=cfg.ch_port,
        username=cfg.ch_user,
        password=cfg.ch_password,
        database=cfg.ch_db,
        compress=True,
        connect_timeout=5,
        send_receive_timeout=10,
    )


def get_client(cfg: Config | None = None) -> Client:
    return _client_for(cfg or Config.from_env())


def rows_to_dicts(result: Any) -> list[dict[str, Any]]:
    return [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]
